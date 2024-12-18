from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Argumentos por defecto para el DAG
default_args = {
    'retries': 1,  # Número de reintentos en caso de fallo
}

# Definición del DAG
with DAG(
    'geolocation_pipeline',  # Nombre del DAG
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Cron para ejecutar todos los días a las 00:00
    start_date=datetime(2023, 1, 1),  # Fecha de inicio
    catchup=False  # No ejecutar fechas pasadas automáticamente
) as dag:
    # Tarea que ejecuta el script con spark-submit
    task = BashOperator(
        task_id='run_pipeline',  # Identificador único de la tarea
        bash_command='spark-submit /Users/ihuerta/Desktop/pec/mytraffic/hiring-de-geolocation-data-main/spark_jobs/process_geolocation.py'
    )
