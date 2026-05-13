# OmniSales: Plataforma Big Data Para Análisis Multicanal De Ventas Y Retroalimentación De Clientes

## 1. Descripción del caso

OmniSales es una empresa ficticia de retail que opera a través de múltiples canales de venta:
tienda física Norte, tienda física Sur y canal online. La empresa necesita consolidar y analizar
la información de ventas proveniente de cada canal junto con la retroalimentación de sus clientes,
con el objetivo de detectar tendencias de demanda, categorías más rentables,
comportamiento por segmento de cliente y señales de riesgo en tiempo real
(stock bajo y devoluciones de alto valor).

## 2. Objetivo general

Construir una solución Big Data usando Apache Spark, Python y Kafka para procesar datos
históricos batch (CSV, JSON, TXT) y eventos streaming multicanal, generar KPIs de negocio
y visualizaciones accionables.

## 3. Tecnologías usadas

- Docker
- Python
- Apache Spark / PySpark
- Spark SQL
- RDD
- DataFrames
- Kafka (productor y consumidor)
- CSV, JSON, TXT (formatos de entrada)
- Parquet (formato de salida analítico)
- Matplotlib

## 4. Archivos de datos generados

| Archivo                  | Formato | Descripción                                      |
|--------------------------|---------|--------------------------------------------------|
| `ventas_norte.csv`       | CSV     | 6,000 transacciones del canal físico Norte       |
| `ventas_sur.csv`         | CSV     | 5,000 transacciones del canal físico Sur         |
| `clientes.json`          | JSON    | Catálogo de 3,000 clientes con segmento          |
| `productos.json`         | JSON    | Catálogo de 200 productos con precio y costo     |
| `feedback_tienda.txt`    | TXT     | 800 comentarios pipe-separated de clientes       |

**Total ventas históricas: 11,000 registros**
**Formatos distintos: CSV, JSON, TXT**

**Relaciones entre archivos:**
- `cliente_id` vincula ventas ↔ clientes
- `producto_id` vincula ventas ↔ productos
- `canal` vincula ventas ↔ feedback ↔ eventos streaming
- `fecha` vincula ventas ↔ feedback para análisis temporal

## 5. Tipos de eventos streaming

| Tipo de evento       | Descripción                                          |
|----------------------|------------------------------------------------------|
| `venta_iniciada`     | Cliente inicia proceso de compra                     |
| `venta_completada`   | Pago aprobado y transacción cerrada                  |
| `venta_devuelta`     | Cliente solicita devolución                          |
| `stock_bajo`         | Producto con stock por debajo del umbral (≤ 15 u.)   |
| `feedback_recibido`  | Nueva retroalimentación registrada                   |
| `descuento_aplicado` | Venta con descuento especial detectado               |

**Total tipos de evento: 6**
**Eventos por ejecución: 1,500 por defecto**

## 6. Reglas de alerta streaming

| Alerta                        | Condición                                              |
|-------------------------------|--------------------------------------------------------|
| `ALERTA_STOCK_BAJO`           | Evento `stock_bajo` con `stock_actual` ≤ 15 unidades   |
| `ALERTA_DEVOLUCION_ALTO_VALOR`| Evento `venta_devuelta` con `monto_venta` > S/ 500     |

**(reglas de alerta)**

## 7. KPIs generados (batch)

| Archivo KPI                      | Descripción                                         |
|----------------------------------|-----------------------------------------------------|
| `kpi_ventas_por_canal.csv`       | Ingreso, ticket promedio y margen por canal         |
| `kpi_ventas_por_categoria.csv`   | Rendimiento por categoría de producto               |
| `kpi_clientes_por_segmento.csv`  | Comportamiento de compra por segmento de cliente    |
| `kpi_productos_top.csv`          | Top 20 productos por ingreso total                  |
| `kpi_feedback_por_canal.csv`     | Calificación promedio y sentimiento por canal       |

**(KPIs)**

## 8. Resúmenes streaming

| Archivo                           | Descripción                                         |
|-----------------------------------|-----------------------------------------------------|
| `summary_by_canal.csv`            | Ingreso, ventas y alertas por canal en tiempo real  |
| `summary_by_categoria.csv`        | Monto, devoluciones y stock bajo por categoría      |

**(resumenes)**

## 9. Visualizaciones

| Gráfico                          | Interpreta                                          |
|----------------------------------|-----------------------------------------------------|
| `chart_ventas_por_canal.png`     | Rentabilidad y ticket promedio por canal            |
| `chart_ventas_por_categoria.png` | Ingreso bruto vs margen por categoría               |
| `chart_feedback_por_canal.png`   | Sentimiento de clientes por canal de venta          |

**(graficos)**

## 10. Interpretaciones de resultados

### Interpretación 1 — Canal más rentable
El gráfico `chart_ventas_por_canal.png` permite comparar el ingreso bruto total y el
ticket promedio de cada canal. Si el canal online registra el ticket promedio más alto
pero menor volumen de transacciones, la estrategia recomendada sería invertir en
captación digital manteniendo la experiencia de compra. Si las tiendas físicas lideran
en volumen pero con ticket promedio menor, existe oportunidad de upselling en punto de venta.

### Interpretación 2 — Categorías de producto estratégicas
El gráfico `chart_ventas_por_categoria.png` confronta ingreso bruto vs margen total.
Una categoría con alto ingreso pero margen reducido (electrónica, por ejemplo) indica
competencia de precios y presión en costos. Una categoría con menor ingreso pero mayor
margen relativo (belleza, bebidas) puede ser palanca de rentabilidad si se incrementa
su participación en el mix de ventas.

### Interpretación 3 — Satisfacción del cliente por canal
El gráfico `chart_feedback_por_canal.png` muestra la distribución positivo/neutro/negativo
por canal. Un canal con alta proporción de comentarios negativos es una señal de alerta
operativa: puede indicar tiempos de espera altos, problemas logísticos o productos fuera
de stock. La calificación promedio superpuesta permite priorizar acciones de mejora.

## 11. Estructura del proyecto

```text
omnisales-bigdata/
├── data/
│   ├── raw/
│   │   ├── ventas_norte.csv
│   │   ├── ventas_sur.csv
│   │   ├── clientes.json
│   │   ├── productos.json
│   │   └── feedback_tienda.txt
│   ├── processed/
│   │   └── ventas_clean.parquet
│   └── checkpoints/
├── docs/
├── notebooks/
├── output/
│   ├── charts/
│   │   ├── chart_ventas_por_canal.png
│   │   ├── chart_ventas_por_categoria.png
│   │   └── chart_feedback_por_canal.png
│   ├── kpis/
│   │   ├── kpi_ventas_por_canal.csv
│   │   ├── kpi_ventas_por_categoria.csv
│   │   ├── kpi_clientes_por_segmento.csv
│   │   ├── kpi_productos_top.csv
│   │   └── kpi_feedback_por_canal.csv
│   └── streaming/
│       ├── alerts/
│       ├── events/
│       ├── summary_by_canal.csv
│       └── summary_by_categoria.csv
├── src/
│   ├── 01_generate_batch_data.py
│   ├── 02_generate_streaming_events.py
│   ├── 03_batch_etl_spark.py
│   ├── 04_streaming_spark_kafka.py
│   └── 05_visualizations.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```