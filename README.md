# Tarea 3 - Big Data (UNAD) - Procesamiento de datos con Apache Spark

Procesamiento en batch con PySpark del conjunto de datos **Casos positivos de COVID-19 en Colombia**
(Instituto Nacional de Salud, datos.gov.co, id `gt2j-8ykr`).

## ¿Qué hace el código?
`tarea3_batch.py`:
1. **Carga** el CSV desde la carpeta del usuario.
2. **Limpia** los datos: quita duplicados, normaliza textos, cambia vacíos por "SIN DATO", filtra edades y convierte fechas.
3. **Transforma**: crea grupo de edad, mes del reporte, días de recuperación y marca de fallecido.
4. **Analiza (EDA)**: estadísticas de edad, top de departamentos y su letalidad, casos por sexo, estado final, grupo de edad, tipo de contagio, casos por mes, días de recuperación y un ejemplo con RDD (`map` + `reduceByKey`).
5. **Visualiza**: muestra tablas en consola, guarda CSV y gráficas PNG en la carpeta `resultados/`.

## Resultados principales
Se procesaron **328.381 registros** (no se encontraron duplicados).

| Resultado | Valor |
|---|---|
| Departamento con más casos | Bogotá (89.964 casos, letalidad 2,13 %) |
| Mayor letalidad en el top 10 | Atlántico (4,23 %) |
| Grupo de edad con más casos | 30 a 44 años (104.745 casos) |
| Letalidad en menores de 18 años | 0,08 % |
| Letalidad en personas de 75 años o más | 24,9 % |
| Mes con más casos | abril de 2021 (54.812) |
| Municipios con más casos (RDD) | Bogotá, Medellín, Barranquilla, Cali |

Las gráficas generadas están en la carpeta `resultados/`.

## Requisitos
- Máquina virtual del curso con Spark 3.5
- Python 3 con `matplotlib` y `pandas` (`pip install matplotlib pandas`)

## Cómo ejecutarlo
```bash
# 1. Descargar los registros del portal de datos abiertos
wget -O covid19.csv "https://www.datos.gov.co/resource/gt2j-8ykr.csv?\$limit=1000000"

# 2. Ejecutar (el archivo debe quedar en /home/vboxuser/covid19.csv)
spark-submit tarea3_batch.py
```
El archivo se lee desde `/home/vboxuser/covid19.csv`. Si lo tienes en otra ruta o en HDFS,
cambia la variable `RUTA_DATOS` al inicio del código.

## Tiempo real (Anexo 3)
`kafka_producer.py` genera datos simulados de sensores y los envía al topic `sensor_data`.
`spark_streaming_consumer.py` los lee con Spark Structured Streaming y calcula la temperatura y humedad
promedio por sensor en ventanas de 1 minuto.
```bash
python3 kafka_producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
```
