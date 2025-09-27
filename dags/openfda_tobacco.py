from airflow.decorators import dag, task
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
        """
        Busca dados de tobacco por intervalo de datas
        Formato da data: YYYYMMDD (ex: 20230101 para 01/01/2023)
        """
        
        base_url = "https://api.fda.gov/tobacco/problem.json"
        
        # Definir intervalo de datas (ajuste conforme necessidade)
        start_date = "20250701"  # 01 de Janeiro de 2023
        end_date = "20250831"    # 31 de Dezembro de 2023
        
        # URL com busca por intervalo de datas
        url = f"{base_url}?search=date_submitted:[{start_date}+TO+{end_date}]&limit=100"
        
        try:
            # Buscar dados da API
            session = requests.Session()
            response = session.get(url, timeout=30)
            
            if response.status_code != 200:
                return f"Erro API: {response.status_code} - {response.text}"
            
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                return f"Nenhum registro encontrado para o período {start_date} a {end_date}"
            
            # Processar dados baseado na documentação
            records = []
            for record in results:
                processed_record = {
                    # Campo obrigatório: 7-digit ICSR number
                    'report_id': record.get('report_id'),
                    
                    # Data no formato MM/DD/YYYY
                    'date_submitted': record.get('date_submitted'),
                    
                    # Número de produtos de tabaco relatados
                    'number_tobacco_products': record.get('number_tobacco_products', 0),
                    
                    # Array de tipos de produtos de tabaco
                    'tobacco_products': str(record.get('tobacco_products', [])),
                    
                    # Número de problemas de saúde relatados
                    'number_health_problems': record.get('number_health_problems', 0),
                    
                    # Array de termos MedDRA (problemas de saúde)
                    'reported_health_problems': str(record.get('reported_health_problems', [])),
                    
                    # Indica se não-usuários foram afetados
                    'nonuser_affected': record.get('nonuser_affected', 'No information provided'),
                    
                    # Número de problemas com produtos relatados
                    'number_product_problems': record.get('number_product_problems', 0),
                    
                    # Array de problemas com produtos
                    'reported_product_problems': str(record.get('reported_product_problems', [])),
                    
                    # Metadados da busca
                    'search_start_date': start_date,
                    'search_end_date': end_date,
                    'api_url': url
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
                
                # Schema baseado na documentação
                table_schema = [
                    {"name": "report_id", "type": "INTEGER"},
                    {"name": "date_submitted", "type": "STRING"},
                    {"name": "number_tobacco_products", "type": "INTEGER"},
                    {"name": "tobacco_products", "type": "STRING"},
                    {"name": "number_health_problems", "type": "INTEGER"},
                    {"name": "reported_health_problems", "type": "STRING"},
                    {"name": "nonuser_affected", "type": "STRING"},
                    {"name": "number_product_problems", "type": "INTEGER"},
                    {"name": "reported_product_problems", "type": "STRING"},
                    {"name": "search_start_date", "type": "STRING"},
                    {"name": "search_end_date", "type": "STRING"},
                    {"name": "api_url", "type": "STRING"}
                ]
                
                df.to_gbq(
                    destination_table=destination_table,
                    project_id=GCP_PROJECT,
                    if_exists="replace",
                    credentials=credentials,
                    location=BQ_LOCATION,
                    table_schema=table_schema
                )
                
                return f"Sucesso! {len(df)} registros do período {start_date} a {end_date} salvos"
            else:
                return "DataFrame vazio"
                
        except Exception as e:
            return f"Erro: {str(e)}"
    
    fetch_tobacco_by_date_range()

dag = tobacco_date_range_dag()
