from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="primerdag", #nombre del dag ((obligatorio)
    description = "nuestro primer dag", #descripción del dag
    start_date = datetime(2024,3,24), #fecha que comienza el dag, se debe importar datetime
    schedule_interval="@once"  #significa que el proceso se ejecuta una sola vez

) as dag:

    t1 = EmptyOperator(task_id="dummy") #creacion de la tarea con el nombre "dummy"
    t1