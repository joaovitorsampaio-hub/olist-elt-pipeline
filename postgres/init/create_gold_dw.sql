-- Script de criação da camada Gold
BEGIN;

CREATE TABLE IF NOT EXISTS dim_calendario (
    id_data INTEGER PRIMARY KEY,
    data DATE NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL,
    nome_dia VARCHAR(20) NOT NULL,
    is_fim_de_semana INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_clientes (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    city_final VARCHAR(100),
    customer_state CHAR(2),
    regiao VARCHAR(20),
    location_full VARCHAR(255),
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dim_produtos (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_weight_g DOUBLE PRECISION,
    volume_cm3 DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dim_vendedores (
    seller_id VARCHAR(50) PRIMARY KEY,
    city_final VARCHAR(100),
    seller_state CHAR(2),
    location_full VARCHAR(255),
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS fato_vendas (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    horario_venda TIMESTAMP NOT NULL,
    fk_data_venda INTEGER NOT NULL, 
    order_status VARCHAR(20),
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    total_value DECIMAL(10, 2),
    delivery_days INTEGER,
    delay_diff_days INTEGER,
    is_delayed INTEGER,
    
    PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_vendas_calendario FOREIGN KEY (data_venda) REFERENCES dim_calendario(id_data),
    CONSTRAINT fk_vendas_produtos FOREIGN KEY (product_id) REFERENCES dim_produtos(product_id),
    CONSTRAINT fk_vendas_vendedores FOREIGN KEY (seller_id) REFERENCES dim_vendedores(seller_id),
    CONSTRAINT fk_vendas_clientes FOREIGN KEY (customer_id) REFERENCES dim_clientes(customer_id)
);

CREATE TABLE IF NOT EXISTS fato_pagamentos (
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR(20),
    payment_installments INTEGER,
    payment_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS fato_reviews (
    review_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);

-- Tabelas de Inteligência Artificial (Machine Learning)
CREATE TABLE IF NOT EXISTS fato_previsoes_logistica (
    order_id VARCHAR(50) PRIMARY KEY,
    probabilidade_atraso DOUBLE PRECISION,
    alerta_atraso INTEGER
);

CREATE TABLE IF NOT EXISTS dim_ml_feature_importance (
    feature VARCHAR(100) PRIMARY KEY,
    importance DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_vendas_data ON fato_vendas(data_venda);
CREATE INDEX IF NOT EXISTS idx_vendas_cliente ON fato_vendas(customer_id);
CREATE INDEX IF NOT EXISTS idx_vendas_produto ON fato_vendas(product_id);
CREATE INDEX IF NOT EXISTS idx_previsoes_order ON fato_previsoes_logistica(order_id);

COMMIT;
