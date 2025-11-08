from prefect  import Flow,task

@task
def load():
    print("Cargando la aplicacion...")

with Flow("Mi flujo de Prefect") as flow:
    load()

flow.run()

