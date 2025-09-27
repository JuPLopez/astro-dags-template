from airflow.decorators import dag, task
from datetime import date
import pendulum
import requests
import pandas as pd
import logging
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Config
GCP_PROJECT = "ciencia-de-dados-470814"
BQ_DATASET = "enapdatasets"
BQ_TABLE = "tobacco_problems"
BQ_LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"

@dag(
    dag_id="openfda_tobacco_ju",
    schedule="@once",
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    catchup=False,
    default_args={
        "email_on_failure": True,
        "owner": "Juliana Lopez",
        "retries": 1,
    },
    tags=["openfda", "tobacco", "bigquery", "date_range"]
)
def tobacco_date_range_dag():
    
    @task
    def fetch_tobacco_by_date_range():
        start = date(2025, 7, 1)
        end = date(2025, 8, 31)

        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        url = f"https://api.fda.gov/tobacco/problem.json?search=date_submitted:[{start_str}+TO+{end_str}]&limit=100"

        try:
            session = requests.Session()
            response = session.get(url, timeout=30)

            if response.status_code != 200:
                raise Exception(f"Erro API: {response.status_code} - {response.text}")

            data = response.json()
            results = data.get("results", [])

            if not results:
                return f"Nenhum registro encontrado para o período {start_str} a {end_str}"

            records = []
            for record in results:
                processed_record = {
                    'report_id': record.get('report_id'),
                    'date_submitted': record.get('date_submitted'),
                    'number_tobacco_products': record.get('number_tobacco_products', 0),
                    'tobacco_products': str(record.get('tobacco_products', [])),
                    'number_health_problems': record.get('number_health_problems', 0),
                    'reported_health_problems': str(record.get('reported_health_problems', [])),
                    'nonuser_affected': record.get('nonuser_affected', 'No information provided'),
                    'number_product_problems': record.get('number_product_problems', 0),
                    'reported_product_problems': str(record.get('reported_product_problems', [])),
                }
                records.append(processed_record)

            df = pd.DataFrame(records)

            if not df.empty:
                bq_hook = BigQueryHook(
                    gcp_conn_id=GCP_CONN_ID, 
                    location=BQ_LOCATION, 
                    use_legacy_sql=False
                )
                credentials = bq_hook.get_credentials()
                destination_table = f"{BQ_DATASET}.{BQ_TABLE}"

                df.to_gbq(
                    destination_table=destination_table,
                    project_id=GCP_PROJECT,
                    if_exists="append",  # ALTERADO para acumular dados
                    credentials=credentials,
                    location=BQ_LOCATION
                )

                return f"Sucesso! {len(df)} registros salvos de {start_str} a {end_str}"

            else:
                return "DataFrame vazio"

        except Exception as e:
            logging.error("Erro ao buscar ou salvar dados: %s", str(e))
            return f"Erro: {str(e)}"
    
    fetch_tobacco_by_date_range()

dag = tobacco_date_range_dag()

