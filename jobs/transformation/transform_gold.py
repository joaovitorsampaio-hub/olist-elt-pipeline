import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine, text
import boto3

# Configurações de conexão
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'admin12345'
SILVER_BUCKET = 'silver'
GOLD_BUCKET = 'gold'
DB_CONNECTION_STR = "postgresql+psycopg2://postgres:root@postgres_dw:5432/dw"

storage_options = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

engine = create_engine(DB_CONNECTION_STR)

def save_to_minio(df, table_name):
    """Persistência em formato parquet no MinIO."""
    output_path = f"s3://{GOLD_BUCKET}/{table_name}/{table_name}.parquet"
    try:
        df.to_parquet(output_path, index=False, storage_options=storage_options)
    except Exception as e:
        logging.error(f"Erro MinIO {table_name}: {e}")

def save_to_postgres(df, table_name):
    """Carga via TRUNCATE CASCADE e APPEND para preservação de PK/FK/Índices."""
    try:
        with engine.begin() as conn:
            query = text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
            if conn.execute(query).scalar():
                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
                df.to_sql(table_name, conn, if_exists='append', index=False)
            else:
                df.to_sql(table_name, conn, if_exists='replace', index=False)
    except Exception as e:
        logging.error(f"Erro Postgres {table_name}: {e}")

def get_region(state):
    """Mapeamento de estados para regiões geográficas."""
    regions = {
        'Norte': ['AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC'],
        'Nordeste': ['MA', 'PI', 'CE', 'RN', 'PE', 'PB', 'SE', 'AL', 'BA'],
        'Centro-Oeste': ['MT', 'MS', 'GO', 'DF'],
        'Sudeste': ['SP', 'RJ', 'ES', 'MG'],
        'Sul': ['PR', 'RS', 'SC']
    }
    for region, states in regions.items():
        if state in states: return region
    return 'Outros'

def create_dim_calendario():
    """Geração de dimensão temporal com chave substituta YYYYMMDD."""
    dates = pd.date_range(start='2016-01-01', end='2020-12-31', freq='D')
    df = pd.DataFrame({'data': dates})
    
    # Chave substituta inteira para evitar conflito com timestamps
    df['id_data'] = df['data'].dt.strftime('%Y%m%d').astype(int)
    df['ano'] = df['data'].dt.year
    df['mes'] = df['data'].dt.month
    df['dia'] = df['data'].dt.day
    df['trimestre'] = df['data'].dt.quarter
    df['dia_semana'] = df['data'].dt.dayofweek
    df['nome_dia'] = df['data'].dt.day_name()
    df['is_fim_de_semana'] = np.where(df['dia_semana'] >= 5, 1, 0)
    
    save_to_minio(df, 'dim_calendario')
    save_to_postgres(df, 'dim_calendario')

def create_dimensions():
    """Processamento de dimensões de Clientes, Produtos e Vendedores."""
    # Clientes
    df_cust = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_customers/olist_customers.parquet", storage_options=storage_options)
    df_cust['regiao'] = df_cust['customer_state'].apply(get_region)
    dim_cli = df_cust[['customer_id', 'customer_unique_id', 'city_final', 'customer_state', 
                       'regiao', 'location_full', 'geolocation_lat', 'geolocation_lng']].drop_duplicates(subset=['customer_id'])
    save_to_minio(dim_cli, 'dim_clientes')
    save_to_postgres(dim_cli, 'dim_clientes')

    # Produtos
    df_prod = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_products/olist_products.parquet", storage_options=storage_options)
    dim_prod = df_prod[['product_id', 'product_category_name', 'product_weight_g', 'volume_cm3']].drop_duplicates(subset=['product_id'])
    save_to_minio(dim_prod, 'dim_produtos')
    save_to_postgres(dim_prod, 'dim_produtos')

    # Vendedores
    df_sell = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_sellers/olist_sellers.parquet", storage_options=storage_options)
    dim_sell = df_sell[['seller_id', 'city_final', 'seller_state', 'location_full', 
                        'geolocation_lat', 'geolocation_lng']].drop_duplicates(subset=['seller_id'])
    save_to_minio(dim_sell, 'dim_vendedores')
    save_to_postgres(dim_sell, 'dim_vendedores')

def create_facts():
    """Processamento de tabelas fato com normalização de chaves de data."""
    df_orders = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_orders/olist_orders.parquet", storage_options=storage_options)
    df_items = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_order_items/olist_order_items.parquet", storage_options=storage_options)
    df_pay = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_order_payments/olist_order_payments.parquet", storage_options=storage_options)
    df_rev = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_order_reviews/olist_order_reviews.parquet", storage_options=storage_options)
    
    # Fato Vendas
    fato_vendas = pd.merge(df_items, df_orders, on='order_id', how='left')
    fato_vendas['horario_venda'] = pd.to_datetime(fato_vendas['order_purchase_timestamp'])
    
    # FK para dim_calendario baseada em id_data (YYYYMMDD)
    fato_vendas['fk_data_venda'] = fato_vendas['horario_venda'].dt.strftime('%Y%m%d').astype(int)
    
    cols_vendas = ['order_id', 'order_item_id', 'product_id', 'seller_id', 'customer_id',
                   'horario_venda', 'fk_data_venda', 'order_status', 'price', 'freight_value',
                   'total_value', 'delivery_days', 'delay_diff_days', 'is_delayed']
    
    save_to_minio(fato_vendas[cols_vendas], "fato_vendas")
    save_to_postgres(fato_vendas[cols_vendas], "fato_vendas")

    save_to_minio(df_pay, "fato_pagamentos")
    save_to_postgres(df_pay, "fato_pagamentos")

    # Deduplicação de reviews para garantir integridade da PK composta
    df_rev = df_rev.drop_duplicates(subset=['review_id', 'order_id'])
    save_to_minio(df_rev, "fato_reviews")
    save_to_postgres(df_rev, "fato_reviews")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
    if GOLD_BUCKET not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
        s3.create_bucket(Bucket=GOLD_BUCKET)
        
    create_dim_calendario()
    create_dimensions()
    create_facts()
