import datetime

from prefect import Flow, task
import requests
from prefect.utilities.datetimes import retry_delay


@task(log_stdout=True, max_retries=3, retry_delay=datetime.timedelta(seconds=10))
def extract():
    print("Vamos a obtener la respuesta de la API JSONPlaceholder")
    # Realizamos la peticion a la API
    raw = requests.get("https://jsonplaceholder.typicode.com/posts")
    print("INFO: Código de estado de la respuesta: {}".format(raw.status_code))
    ##Formato JSON
    raw = raw.json()
    return raw

#Transoformación de los datos (si es necesario)
@task(log_stdout=True)
def transform(raw):
    print("INFO: Vamos a proceder con la tarea transform")
    transformed = raw[0]['title']
    # En este caso no hacemos ninguna transformación
    return transformed

@task(log_stdout=True)
def load(transformed):
    print()
    print("INFO: Vm,aos a proceder con la tarea load")
    print("*****ATENCION****")
    print(str(transformed))
    print("*****Fin load*****")


with Flow("P1.2 - JSONPlaceholder") as flow:
    raw = extract()
    transform = transform(raw)
    load(transform)

flow.run()
