from airflow import DAG 
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="orquestación",
    description="probando la orquestación",
    schedule_interval="@daily", #para que sea diario el proceso
    start_date=datetime(2024,3,23), #el inicio de la tarea
    end_date=datetime(2024,4,23), #el termino de la tarea
    default_args={"dependes_on_past":True},#la tarea1 del siguiente dia no estará disponible si no se ejecuta el anterior
    max_active_runs=1 #primero tiene que ejecutarse todas las tareas del dia para activar el siguiente dia

) as dag:
    t1 = BashOperator(
        task_id="tarea1",
        bash_command="sleep 2 && echo 'Tarea 1'")
    
    t2 = BashOperator(
        task_id="tarea2",
        bash_command="sleep 2 && echo 'Tarea 2'")
    
    t3 = BashOperator(
        task_id="tarea3",
        bash_command="sleep 2 && echo 'Tarea 3'")
    
    t4 = BashOperator(
        task_id="tarea4",
        bash_command="sleep 2 && echo 'Tarea 3'")
    
    #dependencia
    t1 >> t2 >> [t3,t4]