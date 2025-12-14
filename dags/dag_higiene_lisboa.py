from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuração Padrão
default_args = {
    'owner': 'grupo10',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    'higiene_urbana_lisboa_etl',
    default_args=default_args,
    description='Pipeline ELT para Higiene Urbana de Lisboa',
    schedule_interval='@daily', # Roda todo dia (ajuste conforme necessidade)
    catchup=False,
) as dag:

    # --- CAMADA BRONZE (Ingestão Paralela) ---
    # Usamos o parâmetro 'cwd' (Current Working Directory) para garantir 
    # que o script rode dentro da pasta certa e encontre os caminhos relativos "../dados"
    
    t_geoapi = BashOperator(
        task_id='bronze_geoapi',
        bash_command='python geoapi_freguesias.py',
        cwd='/opt/airflow/codigos/raw' 
    )

    t_ine = BashOperator(
        task_id='bronze_ine',
        bash_command='python ine.py',
        cwd='/opt/airflow/codigos/raw'
    )

    t_cml_eco = BashOperator(
        task_id='bronze_cml_ecopontos',
        bash_command='python camara_municipal_lisboa.py', # Esse script baixa ecopontos e rotas
        cwd='/opt/airflow/codigos/raw'
    )

    t_osm = BashOperator(
        task_id='bronze_osm',
        bash_command='python open_street_map.py',
        cwd='/opt/airflow/codigos/raw'
    )

    t_meteo = BashOperator(
        task_id='bronze_meteo',
        bash_command='python open_meteo_clima.py',
        cwd='/opt/airflow/codigos/raw'
    )

    t_ocorrencias = BashOperator(
        task_id='bronze_ocorrencias',
        bash_command='python ocorrencias_minha_rua.py',
        cwd='/opt/airflow/codigos/raw'
    )

    # --- CAMADA PRATA (Tratamento) ---
    t_silver = BashOperator(
        task_id='silver_transformation',
        bash_command='python etl_silver.py', # Seu script consolidado do DuckDB
        cwd='/opt/airflow/codigos/prepared'
    )

    # --- CAMADA OURO (Modelagem) ---
    t_gold = BashOperator(
        task_id='gold_analytics',
        bash_command='python etl_gold.py', # Seu script final
        cwd='/opt/airflow/codigos/curated'
    )

    # --- ORQUESTRAÇÃO (Dependências) ---
    # 1. GeoAPI precisa rodar antes do INE (pois o INE usa funções do geoapi) ou em paralelo se independente?
    # No seu código ine.py ele faz: "from geoapi_freguesias import..." 
    # Então o arquivo geoapi_freguesias.py precisa existir, mas a execução dele como script 
    # (para gerar o CSV) pode ser paralela. Vamos colocar paralelo para performance.
    
    [t_geoapi, t_ine, t_cml_eco, t_osm, t_meteo, t_ocorrencias] >> t_silver >> t_gold