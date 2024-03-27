from airflow import DAG
from datetime import datetime
from hellooperator import HelloOperator #importar el operador creado en el otro archivo python

with DAG(
    dag_id="customoperator",
    description="primer customoperator",
    start_date=datetime(2024,3,22)
) as dag: 
    
    t1=HelloOperator(  #su función viene del operador que creamos en otro archivo python
        task_id="hello",
        name="freddy") 
    
    t1