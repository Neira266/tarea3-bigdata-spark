# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD) - Procesamiento en batch con Apache Spark
Conjunto de datos: Casos positivos de COVID-19 en Colombia (datos.gov.co, id gt2j-8ykr)

Que hace este script:
  1. Carga el archivo CSV desde la carpeta del usuario.
  2. Limpia los datos (duplicados, vacios, textos, edades raras, fechas).
  3. Crea columnas nuevas (grupo de edad, mes, dias de recuperacion).
  4. Hace el analisis exploratorio (EDA) con DataFrames y un ejemplo con RDD.
  5. Muestra los resultados en consola, los guarda en CSV y crea graficas PNG.

Como se ejecuta:
  spark-submit tarea3_batch.py
"""

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

# ---------------------------------------------------------------------------
# 0. CONFIGURACION
# Ruta del archivo en la carpeta del usuario. Si lo subes a HDFS cambia por:
# "hdfs://localhost:9000/Tarea3/covid19.csv"
# ---------------------------------------------------------------------------
RUTA_DATOS = "file:///home/vboxuser/covid19.csv"
CARPETA_SALIDA = "resultados"   # aqui quedan los CSV y las graficas

spark = SparkSession.builder.appName("Tarea3_Batch_COVID19").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def titulo(texto):
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)


# ---------------------------------------------------------------------------
# 1. CARGA DE DATOS
# ---------------------------------------------------------------------------
titulo("1. CARGA DEL CONJUNTO DE DATOS")
df = spark.read.csv(RUTA_DATOS, header=True, inferSchema=False, encoding="UTF-8")

# Por si el archivo se descargo con los nombres "bonitos" del portal
# (con tildes y espacios) los pasamos a minuscula y sin espacios.
import unicodedata
def limpiar_nombre(nombre):
    n = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return n.strip().lower().replace(" ", "_")
df = df.toDF(*[limpiar_nombre(c) for c in df.columns])

print("Columnas:", df.columns)
total_inicial = df.count()
print("Numero de registros cargados:", total_inicial)
df.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 2. LIMPIEZA
# ---------------------------------------------------------------------------
titulo("2. LIMPIEZA DE DATOS")

# 2.1 Quitar registros repetidos (mismo id de caso)
df = df.dropDuplicates(["id_de_caso"])
print("Registros despues de quitar duplicados:", df.count())

# 2.2 Textos en mayuscula y sin espacios sobrantes
for c in ["departamento_nom", "ciudad_municipio_nom", "sexo",
          "fuente_tipo_contagio", "ubicacion", "estado", "recuperado"]:
    df = df.withColumn(c, F.upper(F.trim(F.col(c))))

# 2.3 Valores vacios o "N/A" se reemplazan por "SIN DATO"
for c in ["sexo", "fuente_tipo_contagio", "ubicacion", "estado", "recuperado"]:
    df = df.withColumn(
        c, F.when((F.col(c).isNull()) | (F.col(c).isin("", "N/A", "NA")), "SIN DATO")
            .otherwise(F.col(c)))

# 2.4 Edad a numero. Solo dejamos edades en anos (unidad_medida = 1) y entre 0 y 110
df = df.withColumn("edad", F.col("edad").cast(IntegerType()))
df = df.withColumn("unidad_medida", F.col("unidad_medida").cast(IntegerType()))
df = df.withColumn("edad", F.when(F.col("unidad_medida") == 1, F.col("edad")).otherwise(F.lit(0)))
df = df.filter(F.col("edad").isNotNull() & (F.col("edad") >= 0) & (F.col("edad") <= 110))

# 2.5 Fechas de texto a tipo fecha
for c in ["fecha_reporte_web", "fecha_diagnostico", "fecha_recuperado",
          "fecha_inicio_sintomas"]:
    if c in df.columns:
        df = df.withColumn(c, F.to_date(F.substring(F.col(c), 1, 10)))

df = df.cache()
total_limpio = df.count()
print("Registros despues de la limpieza:", total_limpio)
print("Registros eliminados:", total_inicial - total_limpio)

# ---------------------------------------------------------------------------
# 3. TRANSFORMACIONES (columnas nuevas)
# ---------------------------------------------------------------------------
titulo("3. TRANSFORMACIONES")
df = df.withColumn(
    "grupo_edad",
    F.when(F.col("edad") < 18, "0-17")
     .when(F.col("edad") < 30, "18-29")
     .when(F.col("edad") < 45, "30-44")
     .when(F.col("edad") < 60, "45-59")
     .when(F.col("edad") < 75, "60-74")
     .otherwise("75+"))
df = df.withColumn("mes_reporte", F.date_format("fecha_reporte_web", "yyyy-MM"))
df = df.withColumn("dias_recuperacion",
                   F.datediff("fecha_recuperado", "fecha_diagnostico"))
df = df.withColumn("es_fallecido",
                   F.when(F.col("recuperado") == "FALLECIDO", 1).otherwise(0))
df.select("id_de_caso", "edad", "grupo_edad", "mes_reporte",
          "dias_recuperacion", "es_fallecido").show(5)

# ---------------------------------------------------------------------------
# 4. ANALISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------------------
titulo("4.1 ESTADISTICAS DE LA EDAD")
df.select("edad").describe().show()

titulo("4.2 TOP 10 DEPARTAMENTOS CON MAS CASOS")
por_depto = (df.groupBy("departamento_nom")
               .agg(F.count("*").alias("casos"),
                    F.sum("es_fallecido").alias("fallecidos"))
               .withColumn("letalidad_%",
                           F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
               .orderBy(F.desc("casos")))
por_depto.show(10, truncate=False)

titulo("4.3 CASOS POR SEXO")
por_sexo = df.groupBy("sexo").count().orderBy(F.desc("count"))
por_sexo.show()

titulo("4.4 ESTADO FINAL DEL PACIENTE (RECUPERADO / FALLECIDO / ACTIVO)")
por_estado = df.groupBy("recuperado").count().orderBy(F.desc("count"))
por_estado.show()

titulo("4.5 CASOS Y LETALIDAD POR GRUPO DE EDAD")
por_edad = (df.groupBy("grupo_edad")
              .agg(F.count("*").alias("casos"),
                   F.sum("es_fallecido").alias("fallecidos"))
              .withColumn("letalidad_%",
                          F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
              .orderBy("grupo_edad"))
por_edad.show()

titulo("4.6 TIPO DE CONTAGIO")
df.groupBy("fuente_tipo_contagio").count().orderBy(F.desc("count")).show()

titulo("4.7 CASOS POR MES")
por_mes = df.groupBy("mes_reporte").count().orderBy("mes_reporte")
por_mes.show(50)

titulo("4.8 PROMEDIO DE DIAS DE RECUPERACION (TOP 10 DEPARTAMENTOS)")
dias = (df.filter((F.col("dias_recuperacion") >= 0) & (F.col("dias_recuperacion") <= 120))
          .groupBy("departamento_nom")
          .agg(F.round(F.avg("dias_recuperacion"), 1).alias("promedio_dias"),
               F.count("*").alias("recuperados"))
          .orderBy(F.desc("recuperados")))
dias.show(10, truncate=False)

# Ejemplo con RDD: map + reduceByKey (contar casos por ciudad)
titulo("4.9 EJEMPLO CON RDD: TOP 10 MUNICIPIOS (map + reduceByKey)")
rdd = df.select("ciudad_municipio_nom").rdd
top_municipios = (rdd.map(lambda fila: (fila[0], 1))
                     .reduceByKey(lambda a, b: a + b)
                     .sortBy(lambda x: x[1], ascending=False)
                     .take(10))
for municipio, casos in top_municipios:
    print(f"{municipio:<30} {casos:>10}")

# ---------------------------------------------------------------------------
# 5. GUARDAR RESULTADOS Y GRAFICAS
# ---------------------------------------------------------------------------
titulo("5. GUARDANDO RESULTADOS")
os.makedirs(CARPETA_SALIDA, exist_ok=True)
tablas = {"casos_por_departamento": por_depto, "casos_por_sexo": por_sexo,
          "estado_final": por_estado, "casos_por_grupo_edad": por_edad,
          "casos_por_mes": por_mes, "dias_recuperacion": dias}
pdfs = {}
for nombre, tabla in tablas.items():
    pdf = tabla.toPandas()          # son tablas pequenas (ya resumidas)
    pdf.to_csv(os.path.join(CARPETA_SALIDA, nombre + ".csv"), index=False)
    pdfs[nombre] = pdf
print("CSV guardados en la carpeta:", CARPETA_SALIDA)

try:
    import matplotlib
    matplotlib.use("Agg")           # para que funcione sin pantalla (por Putty)
    import matplotlib.pyplot as plt

    d = pdfs["casos_por_departamento"].head(10)
    plt.figure(figsize=(10, 5))
    plt.barh(d["departamento_nom"][::-1], d["casos"][::-1], color="#1f77b4")
    plt.title("Top 10 departamentos con mas casos de COVID-19")
    plt.xlabel("Casos")
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_SALIDA, "1_top_departamentos.png"))
    plt.close()

    e = pdfs["casos_por_grupo_edad"]
    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.bar(e["grupo_edad"], e["casos"], color="#2ca02c")
    ax1.set_ylabel("Casos")
    ax2 = ax1.twinx()
    ax2.plot(e["grupo_edad"], e["letalidad_%"], color="#d62728", marker="o")
    ax2.set_ylabel("Letalidad (%)")
    plt.title("Casos y letalidad por grupo de edad")
    fig.tight_layout()
    plt.savefig(os.path.join(CARPETA_SALIDA, "2_grupo_edad.png"))
    plt.close()

    m = pdfs["casos_por_mes"].dropna()
    plt.figure(figsize=(11, 5))
    plt.plot(m["mes_reporte"], m["count"], marker="o")
    plt.xticks(rotation=90)
    plt.title("Casos reportados por mes")
    plt.ylabel("Casos")
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_SALIDA, "3_casos_por_mes.png"))
    plt.close()

    s = pdfs["casos_por_sexo"]
    plt.figure(figsize=(6, 6))
    plt.pie(s["count"], labels=s["sexo"], autopct="%1.1f%%")
    plt.title("Casos por sexo")
    plt.savefig(os.path.join(CARPETA_SALIDA, "4_casos_por_sexo.png"))
    plt.close()
    print("Graficas PNG guardadas en la carpeta:", CARPETA_SALIDA)
except ImportError:
    print("No esta instalado matplotlib. Instala con: pip install matplotlib pandas")

titulo("PROCESO TERMINADO")
spark.stop()
