from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# --- Cofigurações ---
# Se uma tarefa falhar, ele tenta de novo 2 vezes, com intervalo de 1 min. 

default_args = {
    'owner' : 'joaov',
    'depends_on_past' : False,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=1)
}

#--- Definição do DAG ---
with DAG(
    'olist_etl_full', # Id no airflow
    default_args = default_args,
    description = 'Pipeline Olist (MYSQL -> Bronze -> Silver -> Gold)',
    schedule_interval = '0 8 * * *',
    start_date = datetime(2024, 1, 1),
    catchup = False,
    tags = ['olist', 'elt']
) as dag:

    # --- Ingestão (Bronze) ---
    task_bronze = BashOperator(
        task_id ='ingest_bronze',
        bash_command = 'python /opt/airflow/jobs/ingestion/ingest_bronze.py',
        doc_md = "# Ingestão Bronze \nLê do MYSQL e Salva Parquet raw no MinIO." 
        )
    
    # --- Transformação Silver ---
    task_silver = BashOperator(
        task_id = 'transform_silver',
        bash_command = 'python /opt/airflow/jobs/transformation/transform_silver.py',
        doc_md = "# Transformação Silver \nLimpeza e feature engineering (ML.)"
    )

    #--- Transformação Gold --- 
    task_gold = BashOperator(
           task_id = 'transform_gold',
           bash_command = 'python /opt/airflow/jobs/transformation/transform_gold.py',
           doc_md = '# Modelagem Gold\nCriação de Fatos e Dimensões no Postgres (DW). '
    )
    
    # --- Predição de atraso --- 
    task_predict = BashOperator(
        task_id='predict_delay',
        bash_command='python /opt/airflow/jobs/ml/predict_delay.py',
        doc_md="## Inteligência Artificial\nAplica o modelo treinado para prever riscos de atraso na Gold."
    )


    #--- Orquestração ---
    task_bronze >> task_silver >> task_gold >> task_predict
 

