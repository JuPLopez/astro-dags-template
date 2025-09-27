"""
OpenFDA DAG para extração de dados de eventos de medicamentos
Versão moderna com TaskFlow API (@dag e @task)
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import requests
from typing import Dict, List, Any

# Function to generate query URL for a specific month and year
def generate_query_url(year: int, month: int) -> str:
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

@dag(
    dag_id='fetch_openfda_data_monthly_v2',
    description='Retrieve OpenFDA data monthly for sildenafil citrate events - TaskFlow Version',
    schedule_interval='@monthly',
    start_date=datetime(2020, 11, 1),
    catchup=True,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['openfda', 'healthcare', 'monthly', 'taskflow']
)
def fetch_openfda_data_monthly():
    
    @task
    def fetch_openfda_data(execution_date: datetime) -> List[Dict[str, Any]]:
        """Busca dados da API OpenFDA e processa sumário semanal"""
        year = execution_date.year
        month = execution_date.month

        print(f"📅 Buscando dados para {year}-{month:02d}")
        
        query_url = generate_query_url(year, month)
        print(f"🔗 URL da consulta: {query_url}")
        
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
                        
                        print(f"📊 Dados processados: {len(weekly_sum)} semanas")
                        print(weekly_sum.head())
                        
                        # Retorna os dados como lista de dicionários
                        return weekly_sum.to_dict('records')
                    else:
                        print("⚠️ Colunas 'time' ou 'count' não encontradas nos dados")
                        return []
                else:
                    print("ℹ️ Nenhum resultado encontrado para o período")
                    return []
            else:
                print(f"❌ Erro na API: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            print(f"💥 Erro ao buscar dados: {str(e)}")
            return []

    @task
    def save_to_postgresql(weekly_data: List[Dict[str, Any]]) -> str:
        """Salva dados no PostgreSQL"""
        
        if not weekly_data or len(weekly_data) == 0:
            print("📭 Nenhum dado para salvar")
            return "Nenhum dado processado"
        
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            
            df = pd.DataFrame.from_records(weekly_data)
            print(f"💾 Salvando {len(df)} registros no PostgreSQL")
            print(df.head())
            
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
            
            success_msg = f"✅ Dados salvos com sucesso! {len(df)} registros"
            print(success_msg)
            return success_msg
            
        except Exception as e:
            error_msg = f"💥 Erro ao salvar no PostgreSQL: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    # Fluxo da DAG usando TaskFlow
    execution_date = datetime.now()  # Será substituído pelo contexto de execução real
    
    # Chain das tasks
    weekly_data = fetch_openfda_data(execution_date)
    save_result = save_to_postgresql(weekly_data)
    
    # Dependency é automática com o retorno, mas podemos ser explícitos
    weekly_data >> save_result

# Instancia a DAG
openfda_dag = fetch_openfda_data_monthly()

# Debug no carregamento
if __name__ == "__main__":
    print(f"✅ DAG '{openfda_dag.dag_id}' criada com sucesso!")
