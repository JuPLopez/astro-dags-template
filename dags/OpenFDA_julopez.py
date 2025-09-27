"""
OpenFDA DAG para extração de dados de eventos de medicamentos
DAG para buscar dados mensais da API OpenFDA e salvar em PostgreSQL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

# Function to generate query URL for a specific month and year
def generate_query_url(year, month):
    """Gera URL de consulta para a API OpenFDA"""
    start_date = f"{year}{month:02d}01"
    # Calcula o último dia do mês
    if month == 12:
        end_date = f"{year}1231"
    else:
        next_month = datetime(year, month + 1, 1)
        last_day = next_month - timedelta(days=1)
        end_date = f"{year}{month:02d}{last_day.day:02d}"
    
    query = f"https://api.fda.gov/drug/event.json?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22+AND+receivedate:[{start_date}+TO+{end_date}]&count=receivedate"
    return query

def fetch_openfda_data(**context):
    """Busca dados da API OpenFDA e processa sumário semanal"""
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month

    print(f"Buscando dados para {year}-{month:02d}")
    
    query_url = generate_query_url(year, month)
    print(f"URL da consulta: {query_url}")
    
    try:
        response = requests.get(query_url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Verifica se há resultados
            if 'results' in data and data['results']:
                df = pd.DataFrame(data['results'])
                
                # Converte a coluna time para datetime (assumindo que é timestamp)
                if 'time' in df.columns:
                    df['time'] = pd.to_datetime(df['time'], unit='s')  # assumindo timestamp Unix
                
                # Processa os dados
                if 'count' in df.columns and 'time' in df.columns:
                    # Agrupa por semana e soma as contagens
                    weekly_sum = df.groupby(pd.Grouper(key='time', freq='W'))['count'].sum().reset_index()
                    weekly_sum["time"] = weekly_sum["time"].astype(str)
                    
                    print(f"Dados processados: {len(weekly_sum)} semanas")
                    print(weekly_sum.head())
                    
                    # Retorna os dados para XCom
                    return weekly_sum.to_dict('records')
                else:
                    print("Colunas 'time' ou 'count' não encontradas nos dados")
                    return []
            else:
                print("Nenhum resultado encontrado para o período")
                return []
        else:
            print(f"Erro na API: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        print(f"Erro ao buscar dados: {str(e)}")
        return []

def save_to_postgresql(**context):
    """Salva dados no PostgreSQL"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    # Recupera dados do XCom
    ti = context['ti']
    data_records = ti.xcom_pull(task_ids='fetch_openfda_data')
    
    if data_records and len(data_records) > 0:
        df = pd.DataFrame.from_records(data_records)
        
        print(f"Salvando {len(df)} registros no PostgreSQL")
        print(df.head())
        
        try:
            # Conecta ao PostgreSQL
            pg_hook = PostgresHook(postgres_conn_id='postgres')
            engine = pg_hook.get_sqlalchemy_engine()
            
            # Salva no banco
            df.to_sql(
                'openfda_data', 
                con=engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            print("Dados salvos com sucesso!")
            
        except Exception as e:
            print(f"Erro ao salvar no PostgreSQL: {str(e)}")
            raise
    else:
        print("Nenhum dado para salvar")

# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG
dag = DAG(
    'fetch_openfda_data_monthly',
    default_args=default_args,
    description='Retrieve OpenFDA data monthly for sildenafil citrate events',
    schedule_interval='@monthly',  # Executa mensalmente
    start_date=datetime(2020, 11, 1),
    catchup=True,  # Processa meses passados
    max_active_runs=1,
    tags=['openfda', 'healthcare', 'monthly']
)

# Tasks
fetch_data_task = PythonOperator(
    task_id='fetch_openfda_data',
    python_callable=fetch_openfda_data,
    provide_context=True,
    dag=dag,
)

save_data_task = PythonOperator(
    task_id='save_to_postgresql',
    python_callable=save_to_postgresql,
    provide_context=True,
    dag=dag,
)

# Dependencies
fetch_data_task >> save_data_task
