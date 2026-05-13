"""
Archivo: 04_streaming_spark_kafka.py
Proyecto: OmniSales - Big Data Multicanal De Ventas Y Retroalimentación

Objetivo:
Leer eventos de ventas multicanal desde Kafka usando Spark Structured Streaming,
procesarlos en micro-batches y generar alertas operativas.

Entrada:
- Kafka topic: omnisales-events

Salida:
- output/streaming/events/      ← todos los eventos por batch
- output/streaming/alerts/      ← solo eventos con alerta activa
- output/streaming/summary_by_canal.csv       ← resumen 1 (por canal)
- output/streaming/summary_by_categoria.csv   ← resumen 2 (por categoría)

Reglas de alerta (2 mínimo requerido):
  ALERTA 1 - ALERTA_STOCK_BAJO        : evento stock_bajo con stock_actual <= 15
  ALERTA 2 - ALERTA_DEVOLUCION_ALTO_VALOR : venta_devuelta con monto_venta > 500

Resúmenes streaming (2 mínimo requerido):
  Resumen 1 : eventos por canal con ingreso promedio y alertas detectadas
  Resumen 2 : eventos por categoría con monto promedio y distribución de tipos

Dónde se ejecuta:
Dentro del contenedor Docker del servicio spark.

Comando:
docker compose exec spark python src/04_streaming_spark_kafka.py --duration 120
"""

from pathlib import Path
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType,
)


# ============================================================
# 1. Configuración general
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

STREAMING_OUTPUT_DIR = BASE_DIR / "output" / "streaming"
EVENTS_OUTPUT_DIR    = STREAMING_OUTPUT_DIR / "events"
ALERTS_OUTPUT_DIR    = STREAMING_OUTPUT_DIR / "alerts"
CHECKPOINT_DIR       = BASE_DIR / "data" / "checkpoints" / "omnisales_streaming"

for d in [STREAMING_OUTPUT_DIR, EVENTS_OUTPUT_DIR, ALERTS_OUTPUT_DIR, CHECKPOINT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

KAFKA_TOPIC             = "omnisales-events"
KAFKA_BOOTSTRAP_SERVERS = "broker:19092"
KAFKA_SPARK_PACKAGE     = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

# Umbral de stock para alerta (debe coincidir con el productor)
STOCK_BAJO_UMBRAL = 15


# ============================================================
# 2. Schema de los eventos JSON
# ============================================================

event_schema = StructType([
    StructField("event_id",      StringType(),  True),
    StructField("venta_id",      StringType(),  True),
    StructField("event_type",    StringType(),  True),
    StructField("canal",         StringType(),  True),
    StructField("cliente_id",    StringType(),  True),
    StructField("segmento",      StringType(),  True),
    StructField("producto_id",   StringType(),  True),
    StructField("categoria",     StringType(),  True),
    StructField("cantidad",      IntegerType(), True),
    StructField("precio_base",   DoubleType(),  True),
    StructField("descuento_pct", DoubleType(),  True),
    StructField("precio_final",  DoubleType(),  True),
    StructField("monto_venta",   DoubleType(),  True),
    StructField("metodo_pago",   StringType(),  True),
    StructField("stock_actual",  IntegerType(), True),
    StructField("alerta",        StringType(),  True),
    StructField("event_timestamp", StringType(), True),
])


# ============================================================
# 3. Sesión Spark con conector Kafka
# ============================================================

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("OmniSalesKafkaStructuredStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# 4. Helper para CSV acumulativo
# ============================================================

def append_csv_with_header(pdf, output_file: Path) -> None:
    write_header = not output_file.exists()
    pdf.to_csv(
        output_file,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8",
    )


# ============================================================
# 5. Procesamiento por micro-batch
# ============================================================

def process_batch(batch_df, batch_id: int) -> None:
    """
    Procesa cada micro-batch del stream omnisales-events.

    Por cada batch:
    1. Muestra una muestra de eventos recibidos
    2. Genera Resumen 1: eventos por canal (ingreso promedio, alertas)
    3. Genera Resumen 2: eventos por categoría (monto promedio, distribución)
    4. Aplica las 2 reglas de alerta y guarda los eventos de alerta
    5. Persiste resultados en CSV
    """

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: sin eventos nuevos.")
        return

    print("\n" + "=" * 80)
    print(f"Procesando micro-batch: {batch_id}")
    print("=" * 80)

    batch_df.cache()
    total = batch_df.count()
    print(f"Eventos en este batch: {total:,}")

    print("\nMuestra de eventos recibidos:")
    batch_df.select(
        "event_id", "event_type", "canal", "categoria",
        "monto_venta", "stock_actual", "alerta"
    ).show(8, truncate=False)

    # --------------------------------------------------------
    # RESUMEN 1: por canal
    # Responde: ¿en qué canal se concentra la actividad y el ingreso?
    # --------------------------------------------------------

    resumen_canal_df = (
        batch_df
        .groupBy("canal")
        .agg(
            F.count("*").alias("total_eventos"),
            F.round(F.avg("monto_venta"), 2).alias("monto_promedio"),
            F.round(F.sum("monto_venta"), 2).alias("monto_total"),
            F.sum(
                F.when(F.col("event_type") == "venta_completada", 1).otherwise(0)
            ).alias("ventas_completadas"),
            F.sum(
                F.when(F.col("alerta").isNotNull(), 1).otherwise(0)
            ).alias("alertas_detectadas"),
        )
        .orderBy(F.desc("monto_total"))
    )

    print("\nResumen 1 — Eventos por canal:")
    resumen_canal_df.show(truncate=False)

    # --------------------------------------------------------
    # RESUMEN 2: por categoría de producto
    # Responde: ¿qué categorías generan más movimiento en tiempo real?
    # --------------------------------------------------------

    resumen_categoria_df = (
        batch_df
        .groupBy("categoria")
        .agg(
            F.count("*").alias("total_eventos"),
            F.round(F.avg("monto_venta"), 2).alias("monto_promedio"),
            F.round(F.sum("monto_venta"), 2).alias("monto_total"),
            F.round(F.avg("descuento_pct"), 2).alias("descuento_promedio"),
            F.sum(
                F.when(F.col("event_type") == "stock_bajo", 1).otherwise(0)
            ).alias("eventos_stock_bajo"),
            F.sum(
                F.when(F.col("event_type") == "venta_devuelta", 1).otherwise(0)
            ).alias("devoluciones"),
        )
        .orderBy(F.desc("monto_total"))
    )

    print("\nResumen 2 — Eventos por categoría:")
    resumen_categoria_df.show(truncate=False)

    # --------------------------------------------------------
    # ALERTAS
    # Regla 1: ALERTA_STOCK_BAJO  → stock_actual <= umbral Y tipo stock_bajo
    # Regla 2: ALERTA_DEVOLUCION_ALTO_VALOR → devolución con monto > 500
    # --------------------------------------------------------

    alertas_df = batch_df.filter(
        (
            (F.col("event_type") == "stock_bajo") &
            (F.col("stock_actual") <= STOCK_BAJO_UMBRAL)
        ) | (
            (F.col("event_type") == "venta_devuelta") &
            (F.col("monto_venta") > 500)
        )
    ).select(
        "event_id", "venta_id", "event_type", "canal", "categoria",
        "producto_id", "monto_venta", "stock_actual", "alerta", "event_timestamp"
    ).orderBy(F.desc("monto_venta"))

    total_alertas = alertas_df.count()
    print(f"\nAlertas detectadas en este batch: {total_alertas}")
    if total_alertas > 0:
        alertas_df.show(10, truncate=False)

    # --------------------------------------------------------
    # Persistencia CSV
    # --------------------------------------------------------

    events_pdf = batch_df.toPandas()
    events_pdf["batch_id"] = batch_id
    events_pdf.to_csv(
        EVENTS_OUTPUT_DIR / f"events_batch_{batch_id}.csv",
        index=False, encoding="utf-8"
    )

    if total_alertas > 0:
        alerts_pdf = alertas_df.toPandas()
        alerts_pdf["batch_id"] = batch_id
        alerts_pdf.to_csv(
            ALERTS_OUTPUT_DIR / f"alerts_batch_{batch_id}.csv",
            index=False, encoding="utf-8"
        )

    resumen_canal_pdf = resumen_canal_df.toPandas()
    resumen_canal_pdf["batch_id"] = batch_id
    append_csv_with_header(
        resumen_canal_pdf,
        STREAMING_OUTPUT_DIR / "summary_by_canal.csv"
    )

    resumen_categoria_pdf = resumen_categoria_df.toPandas()
    resumen_categoria_pdf["batch_id"] = batch_id
    append_csv_with_header(
        resumen_categoria_pdf,
        STREAMING_OUTPUT_DIR / "summary_by_categoria.csv"
    )

    batch_df.unpersist()

    print(f"\nArchivos batch {batch_id} guardados en output/streaming/")


# ============================================================
# 6. Función principal
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consumidor Spark Structured Streaming para eventos OmniSales"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="Duración del streaming en segundos"
    )
    args = parser.parse_args()

    print("=" * 80)
    print("Spark Structured Streaming — OmniSales")
    print("=" * 80)
    print(f"Topic Kafka         : {KAFKA_TOPIC}")
    print(f"Bootstrap servers   : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Duración            : {args.duration} segundos")
    print("NOTA: la primera ejecución descarga el conector Kafka automáticamente.")
    print("=" * 80)

    spark = create_spark_session()

    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_stream_df
        .select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("message_value"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("json_data", F.from_json(F.col("message_value"), event_schema))
        .select("message_key", "kafka_timestamp", "json_data.*")
        .withColumn("event_timestamp",    F.to_timestamp("event_timestamp"))
        .withColumn("processing_timestamp", F.current_timestamp())
    )

    query = (
        parsed_df
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("\nStreaming iniciado.")
    print("Ejecuta el productor en otra terminal:")
    print("  docker compose exec spark python src/02_generate_streaming_events.py --events 1500\n")

    query.awaitTermination(args.duration)
    query.stop()

    print("=" * 80)
    print("Streaming finalizado correctamente.")
    print("Resultados en: output/streaming/")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()