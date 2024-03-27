from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def print_hello(): #funcion a definir
    print("hello gente")

with DAG( 
    dag_id="dependencias",
    description="primer DAG creando dependendia entre tareas",
    schedule_interval="@once",
    start_date=datetime(2024,3,22)

) as dag: 
    
    t1=PythonOperator(
        task_id="tarea1",
        python_callable= print_hello)  #aqui hay que pasar la función que hemos implementado
    
    t2=BashOperator(  
        task_id="tarea2",
        bash_command="echo 'tarea2'"
    )

    t3=BashOperator(  
        task_id="tarea3",
        bash_command="echo 'tarea3'"
    )

    t4=BashOperator(  
        task_id="tarea4",
        bash_command="echo 'tarea4'"
    )

    t1 >> t2 >> ([t3,t4]) #Cuando este Ok tarea1 se desbloquea tarea2, cuando este Ok tarea2 se desbloquea tarea3 y tarea4