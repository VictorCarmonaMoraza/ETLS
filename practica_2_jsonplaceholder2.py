import datetime

from prefect import Flow, task
import requests
from prefect.utilities.datetimes import retry_delay


@task(log_stdout=True, max_retries=3, retry_delay=datetime.timedelta(seconds=10))
def extract():
    print("Vamos a obtener la respuesta de la API JSONPlaceholder")
    # Realizamos la peticion a la API
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    print("INFO: Código de estado de la respuesta: {}".format(response.status_code))
    ##Formato JSON
    response = response.json()
    return response


@task(log_stdout=True)
def load(response):
    output = response[0]['title']
    print("INFO: Vm,aos a proceder con la tarea load")
    print("*****ATENCION****")
    print(str(output))
    print("*****Fin load*****")


with Flow("P1.2 - JSONPlaceholder") as flow:
    raw = extract()
    load(raw)

flow.run()
