import datetime
import json

from pendulum import interval
from prefect import Flow, task
import requests
from prefect.utilities.datetimes import retry_delay
from prefect.schedules import IntervalSchedule

# Definimos un schedule que ejecuta el flujo cada 10 segundos
schedule = IntervalSchedule(interval=datetime.timedelta(seconds=10))


@task(log_stdout=True, max_retries=3, retry_delay=datetime.timedelta(seconds=10))
def extract():
    print("Vamos a obtener la respuesta de la API JSONPlaceholder")
    # Realizamos la peticion a la API
    raw = requests.get("https://jsonplaceholder.typicode.com/posts/1")
    print("INFO: Código de estado de la respuesta: {}".format(raw.status_code))
    ##Formato JSON
    raw = json.loads(raw.text)
    with open(".\\raw.json", "w", encoding='utf-8') as file:
        # Escribimos el fichero raw enfile
        json.dump(raw, file,ensure_ascii=False, indent=4)
    return raw

# Transoformación de los datos (si es necesario)
@task(log_stdout=True)
def transform(raw):
    print("******INFO: Vamos a proceder con la tarea transform******")
    transformed = raw['title']
    with open(".\\transformed.json", "w", encoding='utf-8') as file:
        # Escribimos el fichero raw enfile
        json.dump(transformed, file,ensure_ascii=False, indent=4)
    # En este caso no hacemos ninguna transformación
    return transformed


@task(log_stdout=True)
def load(transformed):
    print()
    print("**********INFO: Vamos a proceder con la tarea load*********")
    print("*****ATENCION****")
    print(str(transformed))
    print("*****Fin load*****")


with Flow("P1.2 - JSONPlaceholder",schedule) as flow:
    raw = extract()
    transform = transform(raw)
    load(transform)

flow.run()
