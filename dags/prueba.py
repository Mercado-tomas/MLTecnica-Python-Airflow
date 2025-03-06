## importamos lo necesario
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# funcion hola
def say_hello():
    print("Fucking hello from Airflow!")

# definimos el dag
with DAG(
    dag_id='hola_mundo_dag', #nombre unico del dag
    start_date=datetime(2025,3,4), #fecha de inicio
    schedule_interval=None, #sin schedule, lo ejecutas manualmente
    catchup=False #evita ejecuciones retroactivas
) as dag:

    # tarea del pythonoperator
    hello_task= PythonOperator(
        task_id='say_hello',
        python_callable=say_hello
    )