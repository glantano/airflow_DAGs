from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor #importar sensor


with DAG(dag_id="7.2-externalTaskSensor",
    description="DAG Secundario",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 20),
    end_date=datetime(2022, 8, 25),
    max_active_runs=1 #tiene que ejecutarse toda las tarea del dia para pasar al sgte dia
) as dag:

    t1 = ExternalTaskSensor( 
        task_id="waiting_dag",
		external_dag_id="7.1-externalTaskSensor", #definir al tag que queremos esperar
		external_task_id="tarea_1", #definir la tarea del DAG en concreto que queremos esperar
		poke_interval=10 # Cada 10 segundos pregunta si ya termino la tarea
    )

    t2 = BashOperator(
        task_id="tarea_2",
		bash_command="sleep 10 && echo 'DAG 2 finalizado!'",
		depends_on_past=True
    )

    t1 >> t2