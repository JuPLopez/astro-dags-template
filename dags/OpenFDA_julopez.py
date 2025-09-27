from airflow.decorators import dag, task
from datetime import date
import pendulum
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Config
GCP_PROJECT = "ciencia-de-dados-470814"
BQ_DATASET = "enapdatasets"
BQ_TABLE = "fda"
BQ_LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"

@dag(
    dag_id="openfda_mcb2_test_range",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 23, tz="UTC"),
    catchup=False,
    default_args={
        "email_on_failure": True,
        "owner": "Juliana Lopez",
        "retries": 1,
    },
    tags=["openfda", "bigquery"]
)
def openfda_dag():
    
    @task
    def fetch_and_save():
        # Dados fixos para teste
        start = date(2025, 7, 1)
        end = date(2025, 8, 31)
        drug = "sildenafil+citrate"
        
        # Construir URL
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        url = (f"https://api.fda.gov/drug/event.json"
               f"?search=patient.drug.medicinalproduct:%22{drug}%22"
               f"+AND+receivedate:[{start_str}+TO+{end_str}]&count=receivedate")
        
        # Buscar dados
        session = requests.Session()
        response = session.get(url, timeout=30)
        
        if response.status_code == 404:
            return "Nenhum dado"
        
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        
        if not results:
            return "Sem resultados"
        
        # Processar e salvar
        df = pd.DataFrame(results).rename(columns={"count": "events"})
        df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
        df["win_start"] = pd.to_datetime(start)
        df["win_end"] = pd.to_datetime(end)
        df["drug"] = drug.replace("+", " ")
        
        # BigQuery
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
        
        df.to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
            project_id=GCP_PROJECT,
            if_exists="append",
            credentials=bq_hook.get_credentials(),
            location=BQ_LOCATION
        )
        
        return f"Concluído: {len(df)} registros"
    
    fetch_and_save()

dag = openfda_dag()
