"""
Archivo: 05_visualizations.py
Proyecto: OmniSales - Big Data Multicanal De Ventas Y Retroalimentación

Objetivo:
Generar gráficos a partir de los KPIs creados con Apache Spark.

Gráficos generados (3 mínimo requerido):
1. chart_ventas_por_canal.png
   Ingreso bruto y ticket promedio por canal (tienda_norte, tienda_sur, online).
   Interpreta qué canal es más rentable.

2. chart_ventas_por_categoria.png
   Top de categorías por ingreso bruto y margen total.
   Interpreta qué línea de producto aporta más al negocio.

3. chart_feedback_por_canal.png
   Distribución de calificaciones por canal (positivo / neutro / negativo).
   Interpreta la satisfacción del cliente en cada punto de contacto.

Dónde se ejecuta:
Dentro del contenedor Docker del servicio spark.

Comando:
docker compose exec spark python src/05_visualizations.py
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np


# ============================================================
# 1. Rutas
# ============================================================

BASE_DIR   = Path(__file__).resolve().parents[1]
KPI_DIR    = BASE_DIR / "output" / "kpis"
CHARTS_DIR = BASE_DIR / "output" / "charts"

CHARTS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. Auxiliares
# ============================================================

def read_kpi(filename: str) -> pd.DataFrame:
    path = KPI_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró {path}. "
            "Primero ejecuta src/03_batch_etl_spark.py"
        )
    return pd.read_csv(path)


def save_chart(filename: str) -> None:
    path = CHARTS_DIR / filename
    plt.savefig(path, bbox_inches="tight", dpi=140)
    plt.close()
    print(f"Gráfico creado: output/charts/{filename}")


# Paleta de colores consistente con la identidad OmniSales
COLORS_CANAL     = ["#2B7BB9", "#E07B3A", "#3AAE6E"]
COLORS_CATEGORIA = ["#4A90D9", "#E07B3A", "#3AAE6E", "#9B59B6", "#F1C40F", "#E74C3C", "#1ABC9C"]
COLORS_FEEDBACK  = ["#3AAE6E", "#F1C40F", "#E74C3C"]


# ============================================================
# 3. Gráfico 1: Ventas por canal
#
# Interpretación esperada:
# Muestra el ingreso bruto total y el ticket promedio de cada canal.
# Permite identificar si el canal online supera a las tiendas físicas
# en valor de transacción, o si las tiendas físicas dominan en volumen.
# ============================================================

def chart_ventas_por_canal() -> None:

    df = read_kpi("kpi_ventas_por_canal.csv")

    canales         = df["canal"].tolist()
    ingreso_bruto   = df["ingreso_bruto"].tolist()
    ticket_promedio = df["ticket_promedio"].tolist()

    x      = np.arange(len(canales))
    width  = 0.40

    fig, ax1 = plt.subplots(figsize=(9, 5))

    bars = ax1.bar(x - width / 2, ingreso_bruto, width,
                   label="Ingreso Bruto (S/)", color=COLORS_CANAL)
    ax1.set_xlabel("Canal de Venta")
    ax1.set_ylabel("Ingreso Bruto (S/)")
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"S/ {v:,.0f}"))
    ax1.set_xticks(x)
    ax1.set_xticklabels(canales)

    for bar in bars:
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(ingreso_bruto) * 0.01,
            f"S/ {bar.get_height():,.0f}",
            ha="center", va="bottom", fontsize=8
        )

    ax2 = ax1.twinx()
    ax2.bar(x + width / 2, ticket_promedio, width,
            label="Ticket Promedio (S/)", color=COLORS_CANAL, alpha=0.55)
    ax2.set_ylabel("Ticket Promedio (S/)")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"S/ {v:.1f}"))

    for i, val in enumerate(ticket_promedio):
        ax2.text(
            x[i] + width / 2,
            val + max(ticket_promedio) * 0.02,
            f"S/ {val:.1f}",
            ha="center", va="bottom", fontsize=8, color="#555"
        )

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=8)

    plt.title("Ingreso Bruto y Ticket Promedio Por Canal De Venta")
    plt.tight_layout()
    save_chart("chart_ventas_por_canal.png")


# ============================================================
# 4. Gráfico 2: Ventas por categoría (ingreso + margen)
#
# Interpretación esperada:
# Compara el ingreso bruto y el margen total por categoría de producto.
# Identifica si las categorías de mayor ingreso también son las más rentables,
# o si hay categorías con alto margen pero bajo volumen que merecen atención.
# ============================================================

def chart_ventas_por_categoria() -> None:

    df = read_kpi("kpi_ventas_por_categoria.csv").sort_values(
        "ingreso_bruto", ascending=False
    )

    categorias    = df["categoria"].tolist()
    ingreso_bruto = df["ingreso_bruto"].tolist()
    margen_total  = df["margen_total"].tolist()

    x     = np.arange(len(categorias))
    width = 0.38

    fig, ax = plt.subplots(figsize=(11, 6))

    bars1 = ax.bar(x - width / 2, ingreso_bruto, width,
                   label="Ingreso Bruto", color="#2B7BB9")
    bars2 = ax.bar(x + width / 2, margen_total,  width,
                   label="Margen Total",  color="#3AAE6E", alpha=0.85)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"S/ {v:,.0f}"))
    ax.set_xticks(x)
    ax.set_xticklabels(categorias, rotation=25, ha="right")
    ax.set_xlabel("Categoría de Producto")
    ax.set_ylabel("Monto (S/)")

    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(ingreso_bruto) * 0.008,
                f"S/ {bar.get_height():,.0f}",
                ha="center", va="bottom", fontsize=7)

    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(ingreso_bruto) * 0.008,
                f"S/ {bar.get_height():,.0f}",
                ha="center", va="bottom", fontsize=7, color="#1a6640")

    ax.legend(fontsize=9)
    plt.title("Ingreso Bruto y Margen Total Por Categoría De Producto")
    plt.tight_layout()
    save_chart("chart_ventas_por_categoria.png")


# ============================================================
# 5. Gráfico 3: Feedback por canal (barras apiladas)
#
# Interpretación esperada:
# Muestra la composición del sentimiento de los clientes en cada canal.
# Un canal con alta proporción de comentarios negativos indica un punto
# de mejora prioritario en la experiencia del cliente.
# ============================================================

def chart_feedback_por_canal() -> None:

    df = read_kpi("kpi_feedback_por_canal.csv")

    canales    = df["canal"].tolist()
    positivos  = df["comentarios_positivos"].tolist()
    neutros    = df["comentarios_neutros"].tolist()
    negativos  = df["comentarios_negativos"].tolist()

    x     = np.arange(len(canales))
    width = 0.50

    fig, ax = plt.subplots(figsize=(9, 5))

    b1 = ax.bar(x, positivos, width, label="Positivos (4-5 ★)", color="#3AAE6E")
    b2 = ax.bar(x, neutros,   width, label="Neutros (3 ★)",    color="#F1C40F",
                bottom=positivos)
    b3 = ax.bar(x, negativos, width, label="Negativos (1-2 ★)", color="#E74C3C",
                bottom=[p + n for p, n in zip(positivos, neutros)])

    ax.set_xticks(x)
    ax.set_xticklabels(canales)
    ax.set_xlabel("Canal")
    ax.set_ylabel("Cantidad de Comentarios")

    # Etiquetas de calificación promedio encima de cada barra
    totales = [p + n + ng for p, n, ng in zip(positivos, neutros, negativos)]
    promedios = df["calificacion_promedio"].tolist()
    for i, (total, prom) in enumerate(zip(totales, promedios)):
        ax.text(i, total + max(totales) * 0.02,
                f"Prom: {prom:.2f} ★",
                ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax.legend(fontsize=9, loc="upper right")
    plt.title("Distribución De Feedback Por Canal De Venta")
    plt.tight_layout()
    save_chart("chart_feedback_por_canal.png")


# ============================================================
# 6. Función principal
# ============================================================

def main() -> None:
    print("=" * 70)
    print("Generando visualizaciones del proyecto OmniSales")
    print("=" * 70)

    chart_ventas_por_canal()
    chart_ventas_por_categoria()
    chart_feedback_por_canal()

    print("=" * 70)
    print("Visualizaciones generadas correctamente.")
    print("Gráficos en: output/charts/")
    print("")
    print("Interpretaciones incluidas en cada función:")
    print("  chart_ventas_por_canal.png     → rentabilidad relativa por canal")
    print("  chart_ventas_por_categoria.png → ingreso vs margen por categoría")
    print("  chart_feedback_por_canal.png   → sentimiento de clientes por canal")
    print("=" * 70)


if __name__ == "__main__":
    main()