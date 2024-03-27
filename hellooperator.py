from airflow.models.baseoperator import BaseOperator #importar importar esta clase en customoperator

class HelloOperator(BaseOperator): #la clase operator hereda de Base operator

    def __init__(self, name:str, **kwargs): #constructor
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context): #es la función que ejecuta lo que queremos
        print(f"hola {self.name}")