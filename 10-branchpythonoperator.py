from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator #importar libreria
from datetime import datetime, date

default_args = {
'start_date': datetime(2022, 7, 1),
'end_date': datetime(2022, 8, 1)
}

def _choose(**context):
    if context["logical_date"].date() < date(2022, 7, 15): #la tarea se ejecuta hasta el 15/7
        return "finish_14_june"
    return "start_15_june" #despues del 15/7 se ejecuta esta tarea

with DAG(dag_id="10-branching",
    schedule_interval="@daily",
	default_args=default_args
) as dag:

    branching = BranchPythonOperator(
        task_id="branch",
	    python_callable=_choose)

    finish_14 = BashOperator(   #esta es la tarea si se ejecuta la condición
        task_id="finish_22_june",
	    bash_command="echo 'Running {{ds}}'")

    start_15 = BashOperator(  #esta es la tarea si NO se ejecuta la condición
        task_id="start_23_june",
	    bash_command="echo 'Running {{ds}}'")

    branching >> [finish_14, start_15] #para que el branching tome una tarea o la otra 