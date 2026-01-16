import pandas as pd
import boto3
from io import BytesIO
from sqlalchemy import create_engine
import logging
from datetime import datetime  

#  Configurações 

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"
BUCKET_NAME = "bronze"


# String de conexão SQLAlchemy 
DB_CONNECTION_STR = "mysql+mysqlconnector://root:root@mysql:3306/source_db"

TABLES = [
    "olist_orders",
    "olist_order_items",
    "olist_order_payments",
    "olist_products",
    "olist_customers",
    "olist_sellers",
    "olist_order_reviews",
    "olist_geolocation"
]

def get_minio_client():
    #Cria cliente S3 (Boto3) configurado para o MinIO
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1' # Região dummy 
    )

def ingest_table(table_name):
    print(f" Iniciando ingestão da tabela: {table_name}")
    
    try:
        # 1. Conexão e Leitura do MySQL 
        # O chunksize 
        print("   Lendo do MySQL...")
        engine = create_engine(DB_CONNECTION_STR)
        
        # Ler zip como string
        if "zip_code_prefix" in table_name or "geoloation" in table_name:
            df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
            for col in df.columns:
                if "zip_code" in col:
                    df[col] = df[col].astype(str).str.zfill(5)
        else:
            df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        
        if df.empty:
            print(f"Tabela {table_name} está vazia.")
            return

        print(f" Lido {len(df)} linhas.")

        
        # Adiciona o timestamp 
        df['ingestion_date'] = datetime.now()

        # 2. Conversão para Parquet em Memória 
        print("   Convertendo para Parquet...")
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        
        # 3. Upload para o MinIO
        file_path = f"{table_name}/{table_name}.parquet"
        print(f"   Enviando para MinIO: s3://{BUCKET_NAME}/{file_path}")
        
        s3_client = get_minio_client()
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_path,
            Body=out_buffer.getvalue()
        )
        
        print(f" {table_name} ingerida com metadados.")

    except Exception as e:
        print(f" Erro na tabela {table_name}: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Garante que o bucket existe
    s3 = get_minio_client()
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except:
        print(f"Bucket {BUCKET_NAME} não existe. Tentando criar...")
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
        except Exception as err:
            print(f"Erro crítico ao criar bucket: {err}")
            exit(1)

    for table in TABLES:
        ingest_table(table)