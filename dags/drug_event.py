from airflow.decorators import dag, task
from datetime import date
import pendulum
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Config
GCP_PROJECT = "ciencia-de-dados-470814"
BQ_DATASET = "enapdatasets"
BQ_TABLE = "fda_events3"
BQ_LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"

@dag(
    dag_id="openFDA_julopez_rinvoq",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
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
        # Período com dados reais
        start = date(2023, 1, 1)
        end = date(2023, 1, 31)
        
        # URL da API
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        url = f"https://api.fda.gov/drug/event.json?search=patient.drug.medicinalproduct:rinvoq+AND+receivedate:[{start_str}+TO+{end_str}]"
        
        try:
            # Buscar dados
            session = requests.Session()
            response = session.get(url, timeout=30)
            
            if response.status_code != 200:
                return f"Erro API: {response.status_code}"
            
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                return "Nenhum evento encontrado"
            
            # Processar dados
            records = []
            for event in results:
                record = {
                    'receivedate': event.get('receivedate', ''),
                    'safetyreportid': event.get('safetyreportid', ''),
                    'serious': event.get('serious', ''),
                    'companynumb': event.get('companynumb', ''),
                }
                
                if 'patient' in event:
                    patient = event['patient']
                    record['patientage'] = patient.get('patientage', '')
                    record['patientsex'] = patient.get('patientsex', '')
                
                if 'patient' in event and 'drug' in event['patient']:
                    drugs = event['patient']['drug']
                    if drugs:
                        drug_info = drugs[0]
                        record['medicinalproduct'] = drug_info.get('medicinalproduct', '')
                        record['drugaction'] = drug_info.get('actiondrug', '')
                
                records.append(record)
            
            df = pd.DataFrame(records)
            
            if len(df) > 0:
                # -------- Load to BigQuery using pandas-gbq --------
                # Get auth credentials from Airflow connection
                bq_hook = BigQueryHook(
                    gcp_conn_id=GCP_CONN_ID, 
                    location=BQ_LOCATION, 
                    use_legacy_sql=False
                )
                credentials = bq_hook.get_credentials()
                destination_table = f"{BQ_DATASET}.{BQ_TABLE}"
                
                # Salvar no BigQuery
                df.to_gbq(
                    destination_table=destination_table,
                    project_id=GCP_PROJECT,
                    if_exists="replace",
                    credentials=credentials,
                    location=BQ_LOCATION
                )
                
                return f"Sucesso! {len(df)} eventos salvos em {destination_table}"
            else:
                return "DataFrame vazio"
                
        except Exception as e:
            return f"Erro: {str(e)}"
    
    fetch_and_save()

dag = openfda_dag()
