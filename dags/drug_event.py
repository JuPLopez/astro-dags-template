from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from calendar import monthrange
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Configurações
GCP_PROJECT = "ciencia-de-dados-470814"
BQ_DATASET = "enapdatasets"
BQ_TABLE = "fda_events_rinvoq"
BQ_LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"

def generate_query_url(year: int, month: int) -> str:
    """
    Gera a URL de consulta para a API do OpenFDA para o medicamento Rinvoq
    no mês e ano especificados.
    """
    # Construir [YYYYMMDD TO YYYYMMDD] para o mês inteiro
    start_date = f"{year}{month:02d}01"
    end_day = monthrange(year, month)[1]
    end_date = f"{year}{month:02d}{end_day:02d}"
    
    # Query OpenFDA para Rinvoq, agrupado por data de recebimento
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22rinvoq%22"
        f"+AND+receivedate:[{start_date}+TO+{end_date}]&count=receivedate"
    )


@task
def fetch_openfda_data() -> list[dict]:
    """
    Busca eventos do OpenFDA para o mês lógico do DAG e retorna somas semanais.
    Retorna um objeto JSON-serializável que é automaticamente armazenado no XCom.
    """
    ctx = get_current_context()
    # Airflow 2.x: logical_date é o timestamp da execução
    logical_date = ctx["data_interval_start"]
    year, month = logical_date.year, logical_date.month

    print(f"Fetching OpenFDA data for {year}-{month:02d}")
    
    url = generate_query_url(year, month)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        print(f"Successfully fetched data from OpenFDA")
    except requests.RequestException as e:
        print(f"OpenFDA request failed: {e}")
        return []

    data = resp.json()
    results = data.get("results", [])
    
    if not results:
        print(f"No results found for {year}-{month:02d}")
        return []

    # Criar DataFrame com os resultados
    df = pd.DataFrame(results)
    
    # Renomear coluna 'time' para 'receivedate' para maior clareza
    df = df.rename(columns={'time': 'receivedate'})
    
    # Converter para datetime
    df["receivedate"] = pd.to_datetime(df["receivedate"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["receivedate"])

    if df.empty:
        print("No valid dates after conversion")
        return []

    # Agregação semanal (semana terminando no domingo)
    weekly = (
        df.groupby(pd.Grouper(key="receivedate", freq="W-SUN"))["count"]
        .sum()
        .reset_index()
        .sort_values("receivedate")
    )
    
    # Renomear colunas para melhor descrição
    weekly = weekly.rename(columns={
        "receivedate": "week_end_date",
        "count": "weekly_count"
    })
    
    # Adicionar colunas de ano e mês para referência
    weekly["year"] = weekly["week_end_date"].dt.year
    weekly["month"] = weekly["week_end_date"].dt.month
    
    # Converter datetimes para strings ISO para segurança do XCom
    weekly["week_end_date"] = weekly["week_end_date"].dt.strftime("%Y-%m-%d")

    print(f"Generated {len(weekly)} weekly records for {year}-{month:02d}")
    
    # Retornar lista de registros; TaskFlow colocará isso automaticamente no XCom
    return weekly.to_dict(orient="records")


@task
def save_to_bigquery(rows: list[dict]) -> None:
    """
    Salva as linhas (lista de dicionários) no BigQuery.
    """
    if not rows:
        print("No data to write to BigQuery for this period.")
        return

    import pandas as pd
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    # Criar DataFrame a partir das linhas
    df = pd.DataFrame(rows)
    
    print(f"Preparing to load {len(df)} rows to BigQuery")
    
    # Garantir que a coluna de data está no formato correto
    df['week_end_date'] = pd.to_datetime(df['week_end_date'])
    
    # Adicionar timestamp de processamento
    df['processed_at'] = datetime.now()
    
    # Ordenar colunas para melhor organização
    column_order = ['week_end_date', 'year', 'month', 'weekly_count', 'processed_at']
    df = df[column_order]
    
    print("DataFrame sample:")
    print(df.head())
    
    # Obter hook do BigQuery usando sua connection ID
    bq_hook = BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        use_legacy_sql=False,
        location=BQ_LOCATION
    )
    
    # Obter cliente do BigQuery
    client = bq_hook.get_client()
    
    # ID completo da tabela
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    
    try:
        # Configurar o job de carga
        job_config = {
            'write_disposition': 'WRITE_APPEND',
            'create_disposition': 'CREATE_IF_NEEDED',
        }
        
        print(f"Loading data to BigQuery table: {table_id}")
        
        # Carregar dados para o BigQuery
        job = client.load_table_from_dataframe(
            dataframe=df,
            destination=table_id,
            job_config=job_config
        )
        
        # Aguardar conclusão do job
        job.result()
        
        print(f"✅ Successfully loaded {len(df)} rows to BigQuery table {table_id}")
        
        # Verificar a tabela carregada
        table = client.get_table(table_id)
        print(f"📊 Table {table_id} now has {table.num_rows} total rows")
        
    except Exception as e:
        print(f"❌ Error loading data to BigQuery: {e}")
        raise


@task
def validate_bigquery_load(rows: list[dict]) -> None:
    """
    Tarefa opcional para validar o carregamento no BigQuery.
    """
    if not rows:
        print("No data to validate.")
        return
        
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    
    bq_hook = BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        use_legacy_sql=False,
        location=BQ_LOCATION
    )
    
    client = bq_hook.get_client()
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    
    try:
        # Consulta para verificar os dados mais recentes
        query = f"""
        SELECT 
            COUNT(*) as recent_count,
            MAX(week_end_date) as latest_week
        FROM `{table_id}`
        WHERE DATE(week_end_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            print(f"✅ Validation: {row.recent_count} recent records found")
            print(f"✅ Latest week in table: {row.latest_week}")
            
    except Exception as e:
        print(f"⚠️ Validation query failed: {e}")


# Argumentos padrão do DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="fetch_openfda_rinvoq_data_monthly",
    description="Recupera eventos do OpenFDA para Rinvoq mensalmente e salva somas semanais no BigQuery",
    default_args=default_args,
    schedule="@monthly",  # Executa mensalmente
    start_date=datetime(2023, 11, 1),  # Ajuste conforme necessário
    end_date=None,  # Executa indefinidamente
    catchup=True,   # Processa meses passados
    max_active_runs=1,  # Limita execuções concorrentes
    tags=["openfda", "rinvoq", "bigquery", "farmacovigilancia"],
    doc_md="""
    # DAG de Coleta de Dados do OpenFDA - Rinvoq
    
    Este DAG realiza as seguintes operações:
    
    1. **Busca mensal**: Consulta a API do OpenFDA para eventos relacionados ao medicamento Rinvoq
    2. **Agregação semanal**: Agrupa os dados por semana (terminando no domingo)
    3. **Carga no BigQuery**: Salva os dados agregados no Google BigQuery
    
    **Tabela de destino**: `ciencia-de-dados-470814.enapdatasets.fda_events_rinvoq`
    
    **Estrutura da tabela**:
    - `week_end_date`: Data final da semana (DOMINGO)
    - `year`: Ano de referência
    - `month`: Mês de referência  
    - `weekly_count`: Contagem semanal de eventos
    - `processed_at`: Timestamp de processamento
    
    **Agendamento**: Executa no primeiro dia de cada mês para processar o mês anterior.
    """,
)
def fetch_openfda_rinvoq_data_monthly():
    """
    DAG principal para coleta e processamento de dados do OpenFDA.
    """
    # Buscar dados do OpenFDA
    weekly_data = fetch_openfda_data()
    
    # Salvar no BigQuery
    load_task = save_to_bigquery(weekly_data)
    
    # Validação opcional (pode ser comentada se não for necessária)
    validate_task = validate_bigquery_load(weekly_data)
    
    # Definir dependências
    load_task >> validate_task


# Instanciar o DAG
dag = fetch_openfda_rinvoq_data_monthly()
