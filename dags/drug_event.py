from airflow.decorators import dag, task
from pendulum import datetime
import requests
import pandas as pd
import calendar
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Config GCP/BigQuery
GCP_PROJECT = "ciencia-de-dados-470814"
BQ_DATASET = "enapdatasets"
BQ_TABLE = "fda_events3"
BQ_LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"


@dag(
    dag_id="drug_event",
    description="Recupera dados mensais do OpenFDA e salva no BigQuery",
    start_date=datetime(2020, 1, 1),  # início histórico
    schedule="@monthly",               # executa no 1º dia de cada mês
    catchup=True,                      # executa retroativo desde 2020
    max_active_runs=1,
    default_args={"owner": "juliana", "retries": 1},
    tags=["openfda", "drug", "etl", "bigquery"],
)
def openfda_pipeline():
    """Pipeline mensal que coleta dados do OpenFDA e salva no BigQuery."""

    @task()
    def extract_data(execution_date=None):
        """
        Extrai dados do mês anterior da API OpenFDA e retorna como DataFrame serializado.
        """
        # Calcula mês anterior
        year = execution_date.subtract(months=1).year
        month = execution_date.subtract(months=1).month
        _, last_day = calendar.monthrange(year, month)

        start_date = f"{year}{month:02d}01"
        end_date = f"{year}{month:02d}{last_day:02d}"

        url = (
            "https://api.fda.gov/drug/event.json"
            f"?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22"
            f"+AND+receivedate:[{start_date}+TO+{end_date}]"
            "&count=receivedate"
        )

        response = requests.get(url)
        response.raise_for_status()

        data = response.json().get("results", [])
        df = pd.DataFrame(data)

        return df.to_dict(orient="list")

    @task()
    def load_to_bigquery(df_dict: dict, execution_date=None):
        """
        Carrega os dados no BigQuery.
        """
        df = pd.DataFrame.from_dict(df_dict)

        if df.empty:
            print("Nenhum dado retornado para este período.")
            return "skip"

        # Adiciona colunas de referência
        ref_year = execution_date.subtract(months=1).year
        ref_month = execution_date.subtract(months=1).month
        df["ref_year"] = ref_year
        df["ref_month"] = ref_month

        # Conecta ao BigQuery
        hook = BigQueryHook(
            gcp_conn_id=GCP_CONN_ID,
            use_legacy_sql=False,
            location=BQ_LOCATION,
        )

        # Insere no BigQuery (append mode)
        hook.insert_all(
            project_id=GCP_PROJECT,
            dataset_id=BQ_DATASET,
            table_id=BQ_TABLE,
            rows=df.to_dict(orient="records"),
        )

        return f"{len(df)} linhas carregadas no BigQuery"

    # Encadeamento das tarefas
    raw_data = extract_data()
    load_to_bigquery(raw_data)


# Instancia o DAG
openfda_dag = openfda_pipeline()
