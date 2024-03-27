import pandas as pd

from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.email import EmailOperator

def _generate_platzi_data(**kwargs):
    data = pd.DataFrame({"Student": ["Maria Cruz", "Daniel Crema","Elon Musk", "Karol Castrejon", "Freddy Vega","Felipe Duque"],
        "timestamp": [kwargs['logical_date'],kwargs['logical_date'], 
                    kwargs['logical_date'], kwargs['logical_date'],
                    kwargs['logical_date'],kwargs['logical_date']]})
    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv",header=True, index=False)

with DAG(
    dag_id="Respuesta_Nasa",
    description="Comando bash para simular la respuesta de la NASA de confirmación:",
    start_date=datetime(2023, 1, 1)
    ) as dag:

    task_1 = BashOperator(
        task_id = "Respuesta_Confirmacion_NASA",
        bash_command='sleep 20 && echo "Confirmación de la NASA, pueden proceder" > /tmp/response_{{ds_nodash}}.txt')
    
    task_2 = BashOperator(
        task_id = "Leer_Datos_Respuesta_Nasa",
        bash_command='ls /tmp && head /tmp/response_{{ds_nodash}}.txt')
    
    task_3 = BashOperator(
        task_id = "Obtener_Datos_SPACEX",
        bash_command="curl https://api.spacexdata.com/v4/launches/past > /tmp/spacex_{{ds_nodash}}.json")
    
    task_4 = PythonOperator(
        task_id="Respuesta_Satelite",
        python_callable=_generate_platzi_data)
    
    task_5 = BashOperator(
        task_id = "Leer_Datos_Respuesta_Satelite",
        bash_command='ls /tmp && head /tmp/platzi_data_{{ds_nodash}}.csv')

    email_analistas = EmailOperator(
        task_id='Notificar_Analistas',
        to = "felipeduque9@gmail.com",
        subject = "Notificación Datos finales disponibles",
        html_content = "Notificación para los analistas. Los datos finales están disponibles",
        dag = dag)                 
    
    task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> email_analistas