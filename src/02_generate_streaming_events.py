"""
Archivo: 02_generate_streaming_events.py
Proyecto: OmniSales - Big Data Multicanal De Ventas Y Retroalimentación

Objetivo:
Generar eventos simulados de transacciones de venta multicanal y enviarlos a Kafka.

Este script funciona como productor de Kafka.

Topic usado:
- omnisales-events

Tipos de eventos (4 mínimo requerido):
1. venta_iniciada       ← cliente inicia el proceso de compra
2. venta_completada     ← pago aprobado y venta cerrada
3. venta_devuelta       ← cliente solicita devolución
4. stock_bajo           ← producto con stock por debajo del umbral crítico
5. feedback_recibido    ← nueva retroalimentación registrada
6. descuento_aplicado   ← venta con descuento especial detectado

Eventos generados: entre 1,000 y 3,000 por ejecución (default 1,500)

Dónde se ejecuta:
Dentro del contenedor Docker del servicio spark.

Comando:
docker compose exec spark python src/02_generate_streaming_events.py --events 1500 --delay 0.05
"""

from pathlib import Path
from datetime import datetime
import argparse
import json
import random
import time

import pandas as pd
from confluent_kafka import Producer


# ============================================================
# 1. Configuración de rutas
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR  = BASE_DIR / "data" / "raw"


# ============================================================
# 2. Parámetros generales
# ============================================================

KAFKA_TOPIC             = "omnisales-events"
KAFKA_BOOTSTRAP_SERVERS = "broker:19092"

# Los 6 tipos de evento del dominio OmniSales
EVENT_TYPES = [
    "venta_iniciada",
    "venta_completada",
    "venta_devuelta",
    "stock_bajo",
    "feedback_recibido",
    "descuento_aplicado",
]

EVENT_WEIGHTS = [0.25, 0.30, 0.10, 0.12, 0.10, 0.13]

CANALES        = ["tienda_norte", "tienda_sur", "online"]
CANAL_WEIGHTS  = [0.35, 0.30, 0.35]

CATEGORIAS        = ["electronica", "ropa", "hogar", "alimentos", "deportes", "belleza", "jugueteria"]
CATEGORIA_WEIGHTS = [0.18, 0.22, 0.15, 0.15, 0.12, 0.10, 0.08]

METODOS_PAGO        = ["efectivo", "tarjeta", "transferencia", "yape"]
METODO_PAGO_WEIGHTS = [0.20, 0.40, 0.15, 0.25]

SEGMENTOS        = ["nuevo", "recurrente", "premium", "inactivo"]
SEGMENTO_WEIGHTS = [0.25, 0.50, 0.15, 0.10]

# Umbral de stock para generar alerta stock_bajo
STOCK_BAJO_UMBRAL = 15


# ============================================================
# 3. Carga de datos históricos de referencia
# ============================================================

def load_reference_data() -> dict:
    """
    Carga los archivos históricos generados en el paso 01.

    Usamos:
    - ventas_norte.csv y ventas_sur.csv como base de transacciones
    - clientes.json para segmentos reales
    - productos.json para precios y stock reales

    Si los archivos no existen, lanza un error claro.
    """

    required = {
        "ventas_norte": RAW_DIR / "ventas_norte.csv",
        "ventas_sur":   RAW_DIR / "ventas_sur.csv",
        "clientes":     RAW_DIR / "clientes.json",
        "productos":    RAW_DIR / "productos.json",
    }

    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(
                f"No se encontró {path}. "
                "Primero ejecuta src/01_generate_batch_data.py"
            )

    ventas_norte_df = pd.read_csv(required["ventas_norte"])
    ventas_sur_df   = pd.read_csv(required["ventas_sur"])
    ventas_df       = pd.concat([ventas_norte_df, ventas_sur_df], ignore_index=True)

    with open(required["clientes"], encoding="utf-8") as f:
        clientes = json.load(f)

    with open(required["productos"], encoding="utf-8") as f:
        productos = json.load(f)

    cliente_segmento_map = {c["cliente_id"]: c["segmento"] for c in clientes}
    producto_categoria_map = {p["producto_id"]: p["categoria"] for p in productos}
    producto_precio_map    = {p["producto_id"]: p["precio_base"] for p in productos}
    producto_stock_map     = {p["producto_id"]: p["stock_inicial"] for p in productos}

    return {
        "ventas_df":             ventas_df,
        "cliente_segmento_map":  cliente_segmento_map,
        "producto_categoria_map": producto_categoria_map,
        "producto_precio_map":   producto_precio_map,
        "producto_stock_map":    producto_stock_map,
        "producto_ids":          list(producto_categoria_map.keys()),
        "cliente_ids":           list(cliente_segmento_map.keys()),
    }


# ============================================================
# 4. Callback de entrega Kafka
# ============================================================

def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Error enviando mensaje: {err}")


# ============================================================
# 5. Creación de eventos
# ============================================================

def create_event(event_number: int, ref: dict) -> dict:
    """
    Crea un evento JSON enriquecido para el topic omnisales-events.

    Parámetros:
    - event_number : número secuencial del evento
    - ref          : datos de referencia cargados desde archivos históricos

    Lógica de alertas generadas:
    - ALERTA_STOCK_BAJO    : evento stock_bajo con stock_actual <= STOCK_BAJO_UMBRAL
    - ALERTA_DEVOLUCION    : evento venta_devuelta con monto_venta > 500
    """

    ventas_df             = ref["ventas_df"]
    cliente_segmento_map  = ref["cliente_segmento_map"]
    producto_categoria_map = ref["producto_categoria_map"]
    producto_precio_map   = ref["producto_precio_map"]
    producto_stock_map    = ref["producto_stock_map"]
    producto_ids          = ref["producto_ids"]
    cliente_ids           = ref["cliente_ids"]

    # Tomamos una venta histórica como base
    venta = ventas_df.sample(1).iloc[0]

    event_type  = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    canal       = str(venta["canal"])
    cliente_id  = str(venta["cliente_id"])
    producto_id = str(venta["producto_id"])

    segmento    = cliente_segmento_map.get(cliente_id, "desconocido")
    categoria   = producto_categoria_map.get(producto_id, "desconocido")
    precio_base = float(producto_precio_map.get(producto_id, 50.0))

    # Simulamos stock actual con algo de variación respecto al inicial
    stock_inicial = int(producto_stock_map.get(producto_id, 100))
    stock_actual  = max(0, stock_inicial - random.randint(0, stock_inicial))

    cantidad     = random.randint(1, 6)
    descuento_pct = random.choices([0, 5, 10, 15, 20], weights=[0.40, 0.20, 0.18, 0.12, 0.10], k=1)[0]
    precio_final  = round(precio_base * (1 - descuento_pct / 100), 2)
    monto_venta   = round(precio_final * cantidad, 2)

    # Reglas de alerta:
    # ALERTA 1: stock_bajo con stock_actual crítico
    # ALERTA 2: devolución de alto valor
    alerta = None
    if event_type == "stock_bajo" and stock_actual <= STOCK_BAJO_UMBRAL:
        alerta = "ALERTA_STOCK_BAJO"
    elif event_type == "venta_devuelta" and monto_venta > 500:
        alerta = "ALERTA_DEVOLUCION_ALTO_VALOR"

    event = {
        "event_id":      f"EVT-{event_number:07d}",
        "venta_id":      str(venta["venta_id"]),
        "event_type":    event_type,
        "canal":         canal,
        "cliente_id":    cliente_id,
        "segmento":      segmento,
        "producto_id":   producto_id,
        "categoria":     categoria,
        "cantidad":      cantidad,
        "precio_base":   round(precio_base, 2),
        "descuento_pct": descuento_pct,
        "precio_final":  precio_final,
        "monto_venta":   monto_venta,
        "metodo_pago":   random.choices(METODOS_PAGO, weights=METODO_PAGO_WEIGHTS, k=1)[0],
        "stock_actual":  stock_actual,
        "alerta":        alerta,
        "event_timestamp": datetime.now().isoformat(timespec="seconds"),
    }

    return event


# ============================================================
# 6. Función principal
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Productor Kafka de eventos OmniSales"
    )
    parser.add_argument(
        "--events",
        type=int,
        default=1500,
        help="Cantidad de eventos a enviar (entre 1,000 y 3,000 recomendado)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.05,
        help="Tiempo de espera entre eventos en segundos"
    )
    args = parser.parse_args()

    print("=" * 80)
    print("Productor Kafka - OmniSales")
    print("=" * 80)
    print(f"Topic destino        : {KAFKA_TOPIC}")
    print(f"Bootstrap servers    : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Eventos a enviar     : {args.events}")
    print(f"Delay entre eventos  : {args.delay} segundos")
    print("=" * 80)

    ref = load_reference_data()

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    for n in range(1, args.events + 1):
        event   = create_event(n, ref)
        payload = json.dumps(event, ensure_ascii=False)

        producer.produce(
            topic=KAFKA_TOPIC,
            key=event["venta_id"],
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)

        if n <= 5 or n % 200 == 0:
            print(f"Evento enviado {n:,}: {payload}")

        time.sleep(args.delay)

    producer.flush()

    print("=" * 80)
    print("Envío de eventos finalizado correctamente.")
    print("=" * 80)


if __name__ == "__main__":
    main()