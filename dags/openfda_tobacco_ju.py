from airflow.decorators import dag, task
from datetime import date
import pendulum
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Config
GCP_PROJECT = "ciencia-de-dados-470814"
BQ_DATASET = "enapdatasets"
BQ_TABLE = "tobacco_problems"
BQ_LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"

@dag(
    dag_id="openfda_tobacco",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
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
        # Período com dados reais (mude para um período com dados disponíveis)
        start = date(2023, 7, 1)  # 2023 em vez de 2025 (futuro)
        end = date(2023, 8, 31)   # 2023 em vez de 2025 (futuro)
        
        # URL da API - CORREÇÃO: endpoint correto é tobacco/problem.json
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        url = f"https://api.fda.gov/tobacco/problem.json?search=date_submitted:[{start_str}+TO+{end_str}]&limit=1000"
                       
        try:
            # Buscar dados da API
            session = requests.Session()
            response = session.get(url, timeout=30)
            
            if response.status_code != 200:
                return f"Erro API: {response.status_code} - {response.text}"
            
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                return f"Nenhum registro encontrado para o período {start_str} a {end_str}"
            
            # Processar dados baseado na documentação
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
            
            if len(df) > 0:
                # Salvar no BigQuery
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
                    if_exists="replace",
                    credentials=credentials,
                    location=BQ_LOCATION
                )
                
                return f"Sucesso! {len(df)} registros do período {start_str} a {end_str} salvos"
            else:
                return "DataFrame vazio"
                
        except Exception as e:
            return f"Erro: {str(e)}"
    
    fetch_tobacco_by_date_range()

dag = tobacco_date_range_dag()
