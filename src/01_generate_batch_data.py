"""
Archivo: 01_generate_batch_data.py
Proyecto: OmniSales - Big Data Multicanal De Ventas Y Retroalimentación

Objetivo:
Generar datos históricos ficticios, pero realistas, para una empresa de retail
con múltiples canales de venta (tienda física Norte, tienda física Sur, e-commerce)
y retroalimentación de clientes.

Este script crea los archivos de datos históricos que luego serán
procesados con Apache Spark.

Archivos generados:
- data/raw/ventas_norte.csv       ← ventas del canal físico Norte
- data/raw/ventas_sur.csv         ← ventas del canal físico Sur
- data/raw/clientes.json          ← catálogo de clientes
- data/raw/productos.json         ← catálogo de productos
- data/raw/feedback_tienda.txt    ← retroalimentación de clientes

NOTA:
ventas_norte.csv y ventas_sur.csv son el archivo principal del análisis
y en conjunto superan los 10,000 registros.
La relación entre archivos se establece mediante:
  - cliente_id   (ventas ↔ clientes)
  - producto_id  (ventas ↔ productos)
  - fecha        (ventas ↔ feedback)
  - canal        (ventas ↔ eventos streaming)

Dónde se ejecuta:
Dentro del contenedor Docker del servicio spark.

Comando:
docker compose exec spark python src/01_generate_batch_data.py
"""

from pathlib import Path
from datetime import datetime, timedelta
import json
import random

import numpy as np
import pandas as pd
from faker import Faker


# ============================================================
# 1. Configuración general del script
# ============================================================

SEED = 2026
random.seed(SEED)
np.random.seed(SEED)

fake = Faker("es_ES")
Faker.seed(SEED)

# Ventas por canal (norte + sur > 10,000 registros en total)
N_VENTAS_NORTE = 6_000
N_VENTAS_SUR   = 5_000
N_CLIENTES     = 3_000
N_PRODUCTOS    = 200
N_FEEDBACK     = 800

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR  = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Periodo histórico del análisis: enero–marzo 2026
START_DATE = datetime(2026, 1, 1, 0, 0, 0)
END_DATE   = datetime(2026, 3, 31, 23, 59, 59)


# ============================================================
# 2. Funciones auxiliares
# ============================================================

def random_datetime(start: datetime, end: datetime) -> datetime:
    total_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, total_seconds))


def weighted_choice(values, weights):
    return random.choices(values, weights=weights, k=1)[0]


def save_csv(df: pd.DataFrame, filename: str) -> None:
    path = RAW_DIR / filename
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"{filename} creado: {len(df):,} registros")


def save_json(data: list, filename: str) -> None:
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print(f"{filename} creado: {len(data):,} registros")


def save_txt(lines: list, filename: str) -> None:
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"{filename} creado: {len(lines):,} líneas")


# ============================================================
# 3. Generar catálogo de productos
# ============================================================

def generate_products() -> list:
    """
    Genera el catálogo de productos de OmniSales.

    Campos:
    - producto_id   : identificador único
    - nombre        : nombre del producto
    - categoria     : categoría del producto
    - precio_base   : precio de venta sugerido
    - costo         : costo de adquisición
    - stock_inicial : unidades disponibles al inicio del periodo
    """

    categorias = {
        "electronica":    (150, 1200),
        "ropa":           ( 30,  180),
        "hogar":          ( 25,  350),
        "alimentos":      (  5,   80),
        "deportes":       ( 40,  400),
        "belleza":        ( 15,  120),
        "jugueteria":     ( 20,  200),
    }

    cat_list    = list(categorias.keys())
    cat_weights = [0.18, 0.22, 0.15, 0.15, 0.12, 0.10, 0.08]

    productos = []
    for i in range(1, N_PRODUCTOS + 1):
        categoria = weighted_choice(cat_list, cat_weights)
        precio_min, precio_max = categorias[categoria]
        precio_base = round(float(np.random.uniform(precio_min, precio_max)), 2)
        margen      = round(random.uniform(0.35, 0.60), 2)
        costo       = round(precio_base * (1 - margen), 2)

        productos.append({
            "producto_id":   f"PRD-{i:04d}",
            "nombre":        f"{categoria.capitalize()} {fake.word().capitalize()} {fake.word().capitalize()}",
            "categoria":     categoria,
            "precio_base":   precio_base,
            "costo":         costo,
            "stock_inicial": random.randint(20, 500),
        })

    return productos


# ============================================================
# 4. Generar catálogo de clientes
# ============================================================

def generate_customers() -> list:
    """
    Genera el catálogo de clientes de OmniSales.

    Campos:
    - cliente_id       : identificador único
    - nombre           : nombre completo
    - email            : correo electrónico
    - telefono         : número de contacto
    - ciudad           : ciudad de residencia
    - segmento         : nuevo / recurrente / premium / inactivo
    - canal_preferido  : tienda_norte / tienda_sur / online
    - fecha_registro   : cuándo se registró
    """

    ciudades = ["Lima", "Arequipa", "Trujillo", "Cusco", "Piura",
                "Chiclayo", "Iquitos", "Tacna", "Huancayo", "Puno"]

    segmentos        = ["nuevo", "recurrente", "premium", "inactivo"]
    segmento_weights = [0.25, 0.50, 0.15, 0.10]

    canales        = ["tienda_norte", "tienda_sur", "online"]
    canal_weights  = [0.35, 0.30, 0.35]

    clientes = []
    for i in range(1, N_CLIENTES + 1):
        clientes.append({
            "cliente_id":      f"CLI-{i:05d}",
            "nombre":          fake.name(),
            "email":           f"cliente{i}@omnisales.test",
            "telefono":        fake.phone_number(),
            "ciudad":          random.choice(ciudades),
            "segmento":        weighted_choice(segmentos, segmento_weights),
            "canal_preferido": weighted_choice(canales, canal_weights),
            "fecha_registro":  str(fake.date_between(start_date="-3y", end_date="-1d")),
        })

    return clientes


# ============================================================
# 5. Generar ventas por canal
# ============================================================

def generate_sales(
    canal: str,
    n_ventas: int,
    productos: list,
    clientes: list,
) -> pd.DataFrame:
    """
    Genera ventas históricas para un canal (Norte o Sur).

    Campos:
    - venta_id          : identificador único
    - cliente_id        : referencia al catálogo de clientes
    - producto_id       : referencia al catálogo de productos
    - canal             : tienda_norte | tienda_sur
    - cantidad          : unidades vendidas
    - precio_unitario   : precio real de venta (puede tener descuento)
    - descuento_pct     : porcentaje de descuento aplicado
    - total_venta       : cantidad × precio_unitario
    - metodo_pago       : efectivo / tarjeta / transferencia / yape
    - estado_venta      : completada / devuelta / pendiente
    - fecha_venta       : fecha y hora de la transacción
    - zona_tienda       : zona dentro de la tienda (para análisis espacial)
    """

    cliente_ids  = [c["cliente_id"]  for c in clientes]
    producto_map = {p["producto_id"]: p for p in productos}
    producto_ids = list(producto_map.keys())

    pagos        = ["efectivo", "tarjeta", "transferencia", "yape"]
    pago_weights = [0.20, 0.40, 0.15, 0.25]

    estados        = ["completada", "devuelta", "pendiente"]
    estado_weights = [0.88, 0.08, 0.04]

    zonas = ["zona_a", "zona_b", "zona_c", "zona_d"]

    prefijo = "VN" if canal == "tienda_norte" else "VS"

    rows = []
    for i in range(1, n_ventas + 1):
        producto_id = random.choice(producto_ids)
        producto    = producto_map[producto_id]

        precio_base   = float(producto["precio_base"])
        descuento_pct = round(random.choices(
            [0, 5, 10, 15, 20, 25],
            weights=[0.40, 0.20, 0.18, 0.10, 0.08, 0.04],
            k=1
        )[0], 2)
        precio_unitario = round(precio_base * (1 - descuento_pct / 100), 2)
        cantidad        = random.randint(1, 8)
        total_venta     = round(precio_unitario * cantidad, 2)

        rows.append({
            "venta_id":        f"{prefijo}-{i:07d}",
            "cliente_id":      random.choice(cliente_ids),
            "producto_id":     producto_id,
            "canal":           canal,
            "cantidad":        cantidad,
            "precio_unitario": precio_unitario,
            "descuento_pct":   descuento_pct,
            "total_venta":     total_venta,
            "metodo_pago":     weighted_choice(pagos, pago_weights),
            "estado_venta":    weighted_choice(estados, estado_weights),
            "fecha_venta":     random_datetime(START_DATE, END_DATE),
            "zona_tienda":     random.choice(zonas),
        })

    return pd.DataFrame(rows)


# ============================================================
# 6. Generar feedback de clientes (TXT)
# ============================================================

def generate_feedback(clientes: list) -> list:
    """
    Genera comentarios de retroalimentación de clientes en formato de texto plano.

    Cada línea tiene el formato:
    FECHA|CLIENTE_ID|CANAL|CALIFICACION|COMENTARIO

    La calificación va de 1 a 5.
    Este archivo representa las opiniones recibidas en tienda o por encuesta.
    """

    canales  = ["tienda_norte", "tienda_sur", "online"]
    cliente_ids = [c["cliente_id"] for c in clientes]

    comentarios_positivos = [
        "Excelente atención del personal, volveré pronto.",
        "Los productos son de muy buena calidad.",
        "Proceso de compra rápido y sin complicaciones.",
        "Muy buena variedad de productos disponibles.",
        "El personal fue muy amable y me ayudó a encontrar lo que buscaba.",
        "Precios competitivos y buena experiencia general.",
        "Encontré todo lo que necesitaba, muy completo el catálogo.",
        "La tienda estaba muy ordenada y limpia.",
    ]
    comentarios_neutros = [
        "La experiencia fue correcta, nada especial.",
        "Tiempo de espera aceptable pero podría mejorar.",
        "El producto cumplió las expectativas básicas.",
        "Regular, algunos productos no estaban disponibles.",
        "La atención fue adecuada pero no destacó.",
    ]
    comentarios_negativos = [
        "Tardaron demasiado en atenderme.",
        "El producto llegó con empaque dañado.",
        "Difícil encontrar lo que buscaba, poca señalización.",
        "El personal no supo resolver mi consulta.",
        "Los precios son más altos que en otros lugares.",
        "Muy poca variedad en la categoría que busqué.",
    ]

    lines = ["FECHA|CLIENTE_ID|CANAL|CALIFICACION|COMENTARIO"]

    for _ in range(N_FEEDBACK):
        fecha      = random_datetime(START_DATE, END_DATE).strftime("%Y-%m-%d")
        cliente_id = random.choice(cliente_ids)
        canal      = random.choice(canales)
        calificacion = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.10, 0.15, 0.35, 0.35], k=1)[0]

        if calificacion >= 4:
            comentario = random.choice(comentarios_positivos)
        elif calificacion == 3:
            comentario = random.choice(comentarios_neutros)
        else:
            comentario = random.choice(comentarios_negativos)

        lines.append(f"{fecha}|{cliente_id}|{canal}|{calificacion}|{comentario}")

    return lines


# ============================================================
# 7. Función principal
# ============================================================

def main() -> None:
    print("Generando datos históricos para OmniSales...")
    print("Carpeta de salida:", RAW_DIR)
    print("-" * 60)

    # Catálogos base
    productos = generate_products()
    save_json(productos, "productos.json")

    clientes = generate_customers()
    save_json(clientes, "clientes.json")

    # Ventas por canal (archivos CSV principales)
    ventas_norte_df = generate_sales("tienda_norte", N_VENTAS_NORTE, productos, clientes)
    save_csv(ventas_norte_df, "ventas_norte.csv")

    ventas_sur_df = generate_sales("tienda_sur", N_VENTAS_SUR, productos, clientes)
    save_csv(ventas_sur_df, "ventas_sur.csv")

    total_ventas = N_VENTAS_NORTE + N_VENTAS_SUR
    print(f"Total registros de ventas: {total_ventas:,} (≥ 10,000 ✓)")

    # Feedback en texto plano
    feedback_lines = generate_feedback(clientes)
    save_txt(feedback_lines, "feedback_tienda.txt")

    print("-" * 60)
    print("Proceso finalizado correctamente.")
    print("Archivos generados en data/raw/")
    print("-" * 60)

    print("Vista previa de ventas_norte.csv:")
    print(ventas_norte_df.head(5).to_string(index=False))

    print("-" * 60)
    print("Resumen de ventas Norte por estado:")
    print(ventas_norte_df["estado_venta"].value_counts().to_string())

    print("\nResumen de ventas Sur por estado:")
    print(ventas_sur_df["estado_venta"].value_counts().to_string())


if __name__ == "__main__":
    main()