from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, to_date
import os

def run_pipeline():
    # Ruta base donde están los datos y la salida
    base_path = "/Users/ihuerta/Desktop/pec/mytraffic/hiring-de-geolocation-data-main"
    output_path = os.path.join(base_path, "output", "daily_reports.parquet")

    # Iniciar sesión de Spark
    spark = SparkSession.builder \
        .appName("Geolocation Processing with Postal Codes") \
        .getOrCreate()

    # Cargar los archivos CSV con rutas absolutas
    signals_df = spark.read.csv(os.path.join(base_path, "data/signals.csv"), header=True, inferSchema=True, sep=";")
    demographics_df = spark.read.csv(os.path.join(base_path, "data/sociodemographics.csv"), header=True, inferSchema=True, sep=";")
    postal_codes_df = spark.read.csv(os.path.join(base_path, "data/postal_codes.csv"), header=True, inferSchema=True, sep=";")

    # Filtrar valores nulos y limpiar los datos de señales
    clean_signals_df = signals_df.filter(
        (col("signal_strength").isNotNull()) &
        (col("latitude").isNotNull()) &
        (col("longitude").isNotNull()) &
        (col("battery_level").between(0, 100)) &
        (col("device_status") == "active")
    )

    # Crear condición de cruce para asignar códigos postales a las coordenadas
    join_condition = (
        (col("latitude") >= col("lat_min")) &
        (col("latitude") <= col("lat_max")) &
        (col("longitude") >= col("long_min")) &
        (col("longitude") <= col("long_max"))
    )

    # Unir señales con códigos postales
    mapped_signals_df = clean_signals_df.join(postal_codes_df, join_condition, "left")

    # Filtrar filas que no tienen código postal asignado
    mapped_signals_df = mapped_signals_df.filter(col("postal_code").isNotNull())

    # Unir datos de señales (con código postal) con datos demográficos
    combined_df = mapped_signals_df.join(demographics_df, on="device_id")

    # Agrupar por código postal y fecha para calcular métricas
    aggregated_df = combined_df.groupBy(
        "postal_code", to_date("timestamp").alias("date")
    ).agg(
        count("device_id").alias("visit_count"),  # Número de visitas por código postal
        avg("age").alias("average_age")          # Promedio de edad de los visitantes
    )

    # Verificar que el directorio de salida exista
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))

    # Guardar el resultado en formato Parquet
    aggregated_df.write.parquet(output_path, mode="overwrite")

    # Detener la sesión de Spark
    spark.stop()


if __name__ == "__main__":
    run_pipeline()
