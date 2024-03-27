from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="orquestación_2",
    description="probando la orquestación",
    schedule_interval="0 7 * * 1", #sintaxis cron: cada lunes a las 7 am. Ver URL adjuntada
    start_date=datetime(2024,3,23), 
    end_date=datetime(2024,4,23), 
    default_args={"depends_on_past":True},
    max_active_runs=1 

) as dag:
    t1 = EmptyOperator(
        task_id="tarea1")
    
    t2 = EmptyOperator(
        task_id="tarea2")
    
    t3 = EmptyOperator(
        task_id="tarea3")
    
    t4 = EmptyOperator(
        task_id="tarea4")
    
    #dependencia
    t1 >> t2 >> [t3,t4]