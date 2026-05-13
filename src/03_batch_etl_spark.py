"""
Archivo: 03_batch_etl_spark.py
Proyecto: OmniSales - Big Data Multicanal De Ventas Y Retroalimentación

Objetivo:
Procesar datos históricos multicanal usando Apache Spark.

Proceso ETL:
1. Extract  : lee ventas_norte.csv, ventas_sur.csv, clientes.json,
              productos.json y feedback_tienda.txt desde data/raw/
2. Transform: limpia, enriquece y consolida los datos de múltiples fuentes
3. Load     : guarda el dataset analítico en Parquet y genera 5 KPIs en CSV

KPIs generados:
- kpi_ventas_por_canal.csv          ← ventas totales y ticket promedio por canal
- kpi_ventas_por_categoria.csv      ← rendimiento por categoría de producto
- kpi_clientes_por_segmento.csv     ← análisis del comportamiento por segmento
- kpi_productos_top.csv             ← top 20 productos más vendidos
- kpi_feedback_por_canal.csv        ← calificación promedio y sentimiento por canal

Dónde se ejecuta:
Dentro del contenedor Docker del servicio spark.

Comando:
docker compose exec spark python src/03_batch_etl_spark.py
"""

from pathlib import Path
import json
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
)


# ============================================================
# 1. Rutas del proyecto
# ============================================================

BASE_DIR      = Path(__file__).resolve().parents[1]
RAW_DIR       = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
KPI_DIR       = BASE_DIR / "output" / "kpis"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
KPI_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. Sesión Spark
# ============================================================

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("OmniSalesBatchETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# 3. Funciones auxiliares
# ============================================================

def read_csv(spark: SparkSession, filename: str):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .csv(str(RAW_DIR / filename))
    )


def write_single_csv(df, output_filename: str) -> None:
    """
    Guarda un DataFrame pequeño de Spark como un único CSV en output/kpis/.
    Usa el patrón temp-dir → rename para evitar archivos part-xxxxx.
    """
    final_path = KPI_DIR / output_filename
    temp_dir   = KPI_DIR / f"_tmp_{output_filename.replace('.csv', '')}"

    if final_path.exists():
        final_path.unlink()
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(temp_dir))
    )

    part_files = list(temp_dir.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No se encontró archivo part en {temp_dir}")

    shutil.move(str(part_files[0]), str(final_path))
    shutil.rmtree(temp_dir)
    print(f"KPI creado: output/kpis/{output_filename}")


def show_info(name: str, df) -> None:
    print("-" * 70)
    print(f"DataFrame: {name} | Registros: {df.count():,} | Columnas: {len(df.columns)}")


# ============================================================
# 4. Lectura y carga de archivos JSON y TXT
# ============================================================

def load_json_as_spark_df(spark: SparkSession, filename: str, schema: StructType):
    """
    Lee un archivo JSON (lista de objetos) desde data/raw/ y lo convierte
    en un DataFrame de Spark usando un schema explícito.
    """
    path = RAW_DIR / filename
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    pandas_df = __import__("pandas").DataFrame(data)
    return spark.createDataFrame(pandas_df, schema=schema)


def load_feedback_txt(spark: SparkSession) -> "DataFrame":
    """
    Lee feedback_tienda.txt (pipe-separated) desde data/raw/.
    Formato: FECHA|CLIENTE_ID|CANAL|CALIFICACION|COMENTARIO
    Ignora la línea de encabezado.
    """
    path = RAW_DIR / "feedback_tienda.txt"

    feedback_rdd = spark.sparkContext.textFile(str(path))
    header       = feedback_rdd.first()

    parsed_rdd = (
        feedback_rdd
        .filter(lambda line: line != header and "|" in line)
        .map(lambda line: line.split("|", 4))
        .filter(lambda parts: len(parts) == 5)
        .map(lambda p: (p[0].strip(), p[1].strip(), p[2].strip(), int(p[3].strip()), p[4].strip()))
    )

    return spark.createDataFrame(
        parsed_rdd,
        ["fecha", "cliente_id", "canal", "calificacion", "comentario"]
    )


# ============================================================
# 5. Función principal ETL
# ============================================================

def main() -> None:
    print("=" * 70)
    print("Iniciando ETL batch de OmniSales con Apache Spark")
    print("=" * 70)

    spark = create_spark_session()

    # --------------------------------------------------------
    # 5.1 EXTRACT
    # --------------------------------------------------------

    print("\nLeyendo archivos desde data/raw/ ...")

    # CSV (ventas por canal)
    ventas_norte_df = read_csv(spark, "ventas_norte.csv")
    ventas_sur_df   = read_csv(spark, "ventas_sur.csv")

    show_info("ventas_norte_df", ventas_norte_df)
    show_info("ventas_sur_df",   ventas_sur_df)

    # JSON (catálogos): definimos schemas explícitos para mayor control
    clientes_schema = StructType([
        StructField("cliente_id",      StringType(), True),
        StructField("nombre",          StringType(), True),
        StructField("email",           StringType(), True),
        StructField("telefono",        StringType(), True),
        StructField("ciudad",          StringType(), True),
        StructField("segmento",        StringType(), True),
        StructField("canal_preferido", StringType(), True),
        StructField("fecha_registro",  StringType(), True),
    ])

    productos_schema = StructType([
        StructField("producto_id",   StringType(), True),
        StructField("nombre",        StringType(), True),
        StructField("categoria",     StringType(), True),
        StructField("precio_base",   DoubleType(), True),
        StructField("costo",         DoubleType(), True),
        StructField("stock_inicial", IntegerType(), True),
    ])

    clientes_df  = load_json_as_spark_df(spark, "clientes.json",  clientes_schema)
    productos_df = load_json_as_spark_df(spark, "productos.json", productos_schema)

    show_info("clientes_df",  clientes_df)
    show_info("productos_df", productos_df)

    # TXT (feedback)
    feedback_df = load_feedback_txt(spark)
    show_info("feedback_df", feedback_df)

    # --------------------------------------------------------
    # 5.2 TRANSFORM — Consolidar ventas de ambos canales
    # --------------------------------------------------------

    print("\nConsolidando y limpiando ventas ...")

    ventas_df = (
        ventas_norte_df.union(ventas_sur_df)
        .withColumn("fecha_venta",     F.to_timestamp("fecha_venta"))
        .withColumn("cantidad",        F.col("cantidad").cast("int"))
        .withColumn("precio_unitario", F.col("precio_unitario").cast("double"))
        .withColumn("descuento_pct",   F.col("descuento_pct").cast("double"))
        .withColumn("total_venta",     F.col("total_venta").cast("double"))
        .withColumn("fecha",           F.to_date("fecha_venta"))
        .withColumn("mes",             F.month("fecha_venta"))
        .withColumn("hora",            F.hour("fecha_venta"))
        .withColumn("semana",          F.weekofyear("fecha_venta"))
        .withColumn(
            "rango_descuento",
            F.when(F.col("descuento_pct") == 0, "sin_descuento")
             .when(F.col("descuento_pct") <= 10, "descuento_bajo")
             .when(F.col("descuento_pct") <= 20, "descuento_medio")
             .otherwise("descuento_alto")
        )
    )

    # --------------------------------------------------------
    # 5.3 TRANSFORM — Enriquecimiento con catálogos
    # --------------------------------------------------------

    print("Enriqueciendo ventas con clientes y productos ...")

    clientes_slim_df = clientes_df.select(
        "cliente_id",
        F.col("segmento").alias("segmento_cliente"),
        F.col("ciudad").alias("ciudad_cliente"),
        F.col("canal_preferido").alias("canal_preferido_cliente"),
    )

    productos_slim_df = productos_df.select(
        "producto_id",
        F.col("nombre").alias("nombre_producto"),
        F.col("categoria").alias("categoria_producto"),
        F.col("precio_base").alias("precio_base_producto"),
        F.col("costo").alias("costo_producto"),
    )

    ventas_enriquecidas_df = (
        ventas_df
        .join(clientes_slim_df,  on="cliente_id",  how="left")
        .join(productos_slim_df, on="producto_id", how="left")
        # Margen unitario = precio vendido − costo
        .withColumn(
            "margen_unitario",
            F.round(F.col("precio_unitario") - F.col("costo_producto"), 2)
        )
        .withColumn(
            "margen_total",
            F.round(F.col("margen_unitario") * F.col("cantidad"), 2)
        )
    )

    print("\nVista previa del dataset enriquecido:")
    ventas_enriquecidas_df.select(
        "venta_id", "canal", "categoria_producto",
        "segmento_cliente", "total_venta", "margen_total", "estado_venta"
    ).show(10, truncate=False)

    # --------------------------------------------------------
    # 5.4 LOAD — Dataset analítico en Parquet
    # --------------------------------------------------------

    print("\nGuardando dataset procesado en Parquet ...")
    parquet_path = PROCESSED_DIR / "ventas_clean.parquet"
    if parquet_path.exists():
        shutil.rmtree(parquet_path)
    ventas_enriquecidas_df.write.mode("overwrite").parquet(str(parquet_path))
    print("Archivo Parquet: data/processed/ventas_clean.parquet")

    # --------------------------------------------------------
    # 5.5 Spark SQL — Vista temporal
    # --------------------------------------------------------

    ventas_enriquecidas_df.createOrReplaceTempView("ventas_analytics")
    feedback_df.createOrReplaceTempView("feedback_analytics")

    # --------------------------------------------------------
    # KPI 1: Ventas por canal
    # --------------------------------------------------------

    kpi_canal_df = spark.sql("""
        SELECT
            canal,
            COUNT(*)                                        AS total_transacciones,
            SUM(CASE WHEN estado_venta = 'completada' THEN 1 ELSE 0 END)
                                                            AS ventas_completadas,
            SUM(CASE WHEN estado_venta = 'devuelta'   THEN 1 ELSE 0 END)
                                                            AS ventas_devueltas,
            ROUND(SUM(total_venta), 2)                      AS ingreso_bruto,
            ROUND(AVG(total_venta), 2)                      AS ticket_promedio,
            ROUND(SUM(margen_total), 2)                     AS margen_total,
            ROUND(AVG(descuento_pct), 2)                    AS descuento_promedio_pct
        FROM ventas_analytics
        GROUP BY canal
        ORDER BY ingreso_bruto DESC
    """)
    print("\nKPI 1: Ventas por canal")
    kpi_canal_df.show(truncate=False)
    write_single_csv(kpi_canal_df, "kpi_ventas_por_canal.csv")

    # --------------------------------------------------------
    # KPI 2: Ventas por categoría de producto
    # --------------------------------------------------------

    kpi_categoria_df = spark.sql("""
        SELECT
            categoria_producto                              AS categoria,
            COUNT(*)                                        AS total_transacciones,
            SUM(cantidad)                                   AS unidades_vendidas,
            ROUND(SUM(total_venta), 2)                      AS ingreso_bruto,
            ROUND(AVG(total_venta), 2)                      AS ticket_promedio,
            ROUND(SUM(margen_total), 2)                     AS margen_total,
            ROUND(AVG(descuento_pct), 2)                    AS descuento_promedio_pct
        FROM ventas_analytics
        WHERE estado_venta = 'completada'
        GROUP BY categoria_producto
        ORDER BY ingreso_bruto DESC
    """)
    print("\nKPI 2: Ventas por categoría")
    kpi_categoria_df.show(truncate=False)
    write_single_csv(kpi_categoria_df, "kpi_ventas_por_categoria.csv")

    # --------------------------------------------------------
    # KPI 3: Clientes por segmento
    # --------------------------------------------------------

    kpi_segmento_df = spark.sql("""
        SELECT
            segmento_cliente                                AS segmento,
            COUNT(DISTINCT cliente_id)                      AS clientes_unicos,
            COUNT(*)                                        AS total_compras,
            ROUND(SUM(total_venta), 2)                      AS ingreso_total,
            ROUND(AVG(total_venta), 2)                      AS ticket_promedio,
            ROUND(AVG(descuento_pct), 2)                    AS descuento_promedio_pct
        FROM ventas_analytics
        GROUP BY segmento_cliente
        ORDER BY ingreso_total DESC
    """)
    print("\nKPI 3: Clientes por segmento")
    kpi_segmento_df.show(truncate=False)
    write_single_csv(kpi_segmento_df, "kpi_clientes_por_segmento.csv")

    # --------------------------------------------------------
    # KPI 4: Top 20 productos más vendidos (por ingreso)
    # --------------------------------------------------------

    kpi_top_productos_df = spark.sql("""
        SELECT
            producto_id,
            nombre_producto,
            categoria_producto                              AS categoria,
            SUM(cantidad)                                   AS unidades_vendidas,
            ROUND(SUM(total_venta), 2)                      AS ingreso_total,
            ROUND(AVG(precio_unitario), 2)                  AS precio_promedio,
            ROUND(SUM(margen_total), 2)                     AS margen_total
        FROM ventas_analytics
        WHERE estado_venta = 'completada'
        GROUP BY producto_id, nombre_producto, categoria_producto
        ORDER BY ingreso_total DESC
        LIMIT 20
    """)
    print("\nKPI 4: Top 20 productos")
    kpi_top_productos_df.show(truncate=False)
    write_single_csv(kpi_top_productos_df, "kpi_productos_top.csv")

    # --------------------------------------------------------
    # KPI 5: Feedback por canal (usando RDD para parseo + SQL para agregación)
    # --------------------------------------------------------

    print("\nProcesando feedback_tienda.txt con RDD ...")

    # Usamos RDD para contar líneas por canal directamente
    fb_rdd   = spark.sparkContext.textFile(str(RAW_DIR / "feedback_tienda.txt"))
    fb_hdr   = fb_rdd.first()
    canal_counts = (
        fb_rdd
        .filter(lambda l: l != fb_hdr and "|" in l)
        .map(lambda l: l.split("|"))
        .filter(lambda p: len(p) == 5)
        .map(lambda p: (p[2].strip(), 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    rdd_canal_df = spark.createDataFrame(canal_counts, ["canal", "total_feedback_rdd"])
    print("Conteo de feedback por canal (vía RDD):")
    rdd_canal_df.show()

    kpi_feedback_df = spark.sql("""
        SELECT
            canal,
            COUNT(*)                                        AS total_comentarios,
            ROUND(AVG(calificacion), 2)                     AS calificacion_promedio,
            SUM(CASE WHEN calificacion >= 4 THEN 1 ELSE 0 END)
                                                            AS comentarios_positivos,
            SUM(CASE WHEN calificacion = 3  THEN 1 ELSE 0 END)
                                                            AS comentarios_neutros,
            SUM(CASE WHEN calificacion <= 2 THEN 1 ELSE 0 END)
                                                            AS comentarios_negativos
        FROM feedback_analytics
        GROUP BY canal
        ORDER BY calificacion_promedio DESC
    """)
    print("\nKPI 5: Feedback por canal")
    kpi_feedback_df.show(truncate=False)
    write_single_csv(kpi_feedback_df, "kpi_feedback_por_canal.csv")

    # --------------------------------------------------------
    # Cierre
    # --------------------------------------------------------

    print("=" * 70)
    print("ETL batch finalizado correctamente.")
    print("Dataset analítico : data/processed/ventas_clean.parquet")
    print("KPIs CSV          : output/kpis/")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()