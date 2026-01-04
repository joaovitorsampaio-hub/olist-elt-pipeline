import pandas as pd 
import numpy as np 
import logging
from sqlalchemy import create_engine
import boto3

#--- Configurações ---
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'admin12345'
SILVER_BUCKET = 'silver'
GOLD_BUCKET = 'gold'

# String de conexão com o Postgres (DATA Warehouse)
# Formato: postgresql+psycopg2://user:password@host:port/dbname
DB_CONNECTION_STR="postgresql+psycopg2://postgres:root@postgres_dw:5432/dw"

# Configuração S3 para Pandas
storage_options = {
    "key" : MINIO_ACCESS_KEY,
    "secret" : MINIO_SECRET_KEY,
    "client_kwargs" : {"endpoint_url": MINIO_ENDPOINT}
}

engine=create_engine(DB_CONNECTION_STR)

#--- Funções Auxiliares ---

def save_to_minio(df, table_name):
    # Salva no Data Lake
    output_path = f"s3://{GOLD_BUCKET}/{table_name}/{table_name}.parquet"
    print(f"Salvando {table_name}...")
    try:
        df.to_parquet(output_path, index=False, storage_options=storage_options)
    except Exception as e:
        print(f"Erro MinIO: {e}")

def save_to_postgres(df, table_name):
    # Salva no DW para BI
    print(f"Salvando tabela {table_name}")
    try:
        # Derruba a tabela antiga e cria a nova 
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    except Exception as e:
        print(f"Erro Postgres: {e}")

def get_region(state):
    # Mapeia Região do Brasil
    if state in ['AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC']: 
        return 'Norte'
    if state in ['MA', 'PI', 'CE', 'RN', 'PE', 'PB', 'SE', 'AL', 'BA']: 
        return 'Nordeste'
    if state in ['MT', 'MS','GO','DF']:
        return 'Centro-Oeste'
    if state in ['SP', 'RJ', 'ES', 'MG']:
        return 'Sudeste'
    if state in ['PR', 'RS', 'SC']:
        return 'Sul'
    return 'Outros'

#--- Criação das Dimensões ---

def create_dim_calendario():
    print("\n Criando: Dimensão Calendário")
    dates = pd.date_range(start='2016-01-01', end='2020-12-31', freq='D')
    df=pd.DataFrame({'data': dates})

    df['ano'] = df['data'].dt.year
    df['mes'] = df['data'].dt.month
    df['dia'] = df['data'].dt.day
    df['trimestre'] = df['data'].dt.quarter
    df['dia_semana'] = df['data'].dt.dayofweek # 0=Segunda, 6=Domingo
    df['nome_dia'] = df['data'].dt.day_name()
    df['is_fim_de_semana'] = np.where(df['dia_semana'] >= 5, 1, 0)
    
    save_to_minio(df, 'dim_calendario')
    save_to_postgres(df, 'dim_calendario')

def create_dimensions():
    print("\n Criando Dimensões: clientes, produtos, vendedores")
    
    # Dimensão Clientes
    df_cust = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_customers/olist_customers.parquet",
                              storage_options=storage_options
                              )
    df_cust['regiao'] = df_cust['customer_state'].apply(get_region)
    # Selecionar apenas colunas de contexto (quem/onde)
    dim_cli = df_cust[['customer_id', 'customer_unique_id', 'city_final', 
                       'customer_state', 'regiao', 'location_full', 'geolocation_lat', 'geolocation_lng'
                       ]]
    save_to_minio(dim_cli, 'dim_clientes')
    save_to_postgres(dim_cli, 'dim_clientes')

    # Dimensão Produtos
    df_prod = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_products/olist_products.parquet",
                              storage_options=storage_options
                              )
    # Selecionar colunas descritivas
    dim_prod = df_prod[['product_id', 'product_category_name', 'product_weight_g',
                        'volume_cm3'
                        ]]
    save_to_minio(dim_prod, 'dim_produtos')
    save_to_postgres(dim_prod, 'dim_produtos')

    # Dimensão Vendedores
    df_sell =  pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_sellers/olist_sellers.parquet",
                               storage_options=storage_options
                               )
    dim_sell = df_sell[['seller_id','city_final', 'seller_state',
                        'location_full', 'geolocation_lat', 'geolocation_lng'
                        ]]
    save_to_minio(dim_sell, 'dim_vendedores')
    save_to_postgres(dim_sell, 'dim_vendedores')

#--- Criação das Tabelas Fato ---
def create_facts():
    print('\n Criando: Tabelas Fato')

    # Carregar dados da Silver 
    df_orders = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_orders/olist_orders.parquet",
                                storage_options=storage_options
                                )
    df_items = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_order_items/olist_order_items.parquet",
                               storage_options=storage_options
                               )
    df_pay = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_order_payments/olist_order_payments.parquet",
                             storage_options=storage_options
                             )
    df_rev = pd.read_parquet(f"s3://{SILVER_BUCKET}/olist_order_reviews/olist_order_reviews.parquet",
                             storage_options=storage_options
                             )
    
    #--- Fato Vendas --- 
    # Join items (esquerda) com Orders (direita)
    # Granularidade : 1 linha por item vendido 
    fato_vendas = pd.merge(df_items, df_orders, on='order_id', how='left')

    # Selecionar colunas de métricas e FKs
    cols_fato = ['order_id', 'order_item_id', 'product_id', 'seller_id','customer_id',
                 'order_purchase_timestamp', 'order_status','price','freight_value',
                 'total_value','delivery_days','delay_diff_days','is_delayed'
    ]
    fato_vendas = fato_vendas[cols_fato]
    # Renomear data para facilitar join com dim_calendário
    fato_vendas = fato_vendas.rename(columns={'order_purchase_timestamp' : 'data_venda'})
    save_to_minio(fato_vendas, "fato_vendas")
    save_to_postgres(fato_vendas, "fato_vendas")

    #--- Fato Pagamentos ---
    # Granularidade: 1 linha por transação
    save_to_minio(df_pay, "fato_pagamentos")
    save_to_postgres(df_pay, "fato_pagamentos")

    #--- Fato Reviews ---
    # Granularidade: 1 linha por avaliação
    save_to_minio(df_rev, "fato_reviews")
    save_to_postgres(df_rev, "fato_reviews")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("\n Iniciando Modelagem Gold (Star Schema)")

    # Cria Bucket gold se não existir
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY,
                        aws_secret_access_key=MINIO_SECRET_KEY
                        )
    try:
        s3.head_bucket(Bucket=GOLD_BUCKET)
    except:
        s3.create_bucket(Bucket=GOLD_BUCKET)
        
    create_dim_calendario()
    create_dimensions()
    create_facts()
    print("\n Processamento Gold Concluído! DW atualizado.")