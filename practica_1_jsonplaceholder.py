from prefect import Flow, task
import requests


@task
def extract():
    # Realizamos la peticion a la API
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    ##Formato JSON
    response = response.json()
    return response


@task
def load(response):
    output = response[0]['title']
    print("*****Titulo dle primer onjeto*****")
    print(str(output))
    print("*****Fin load*****")


with Flow("P1.2 - JSONPlaceholder") as flow:
    raw = extract()
    load(raw)

flow.run()
