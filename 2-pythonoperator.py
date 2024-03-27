from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello(): #funcion a definir
    print("hello gente")

with DAG( 
    dag_id="pythonoperator",
    description="primer DAG con Python Operator",
    schedule_interval="@once",
    start_date=datetime(2024,3,22)
) as dag: 
    
    t1=PythonOperator(
        task_id="hello_with_python",
        python_callable= print_hello  #aqui hay que pasar la función que hemos implementado
    )