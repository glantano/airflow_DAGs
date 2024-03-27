from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG( 
    dag_id="bashoperator",
    description="utilizando bash operator",
    start_date=datetime(2024,3,22)
) as dag:
    
    t1 = BashOperator(
        task_id="Hello_with_bash",
        bash_command="echo 'Hello Gente'")
    
    t1