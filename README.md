# Olist ELT Pipeline: Engenharia de Dados e Predição Logística
🇧🇷 Este README está em português.  
🇺🇸 Read this project description in English: [README_EN.md](README_EN.md)

Este projeto apresenta uma **solução completa de Engenharia de Dados** utilizando o dataset público da **Olist**.  
O pipeline implementa o processo **ELT (Extract, Load, Transform)** seguindo a **Arquitetura de Medalhões**, culminando em um **modelo de Machine Learning** capaz de prever atrasos em pedidos quase em tempo real.

---

<img width="784" height="443" alt="Diagrama sem nome drawio" src="https://github.com/user-attachments/assets/46b2fa7c-153c-44ef-be10-ce37bedc54f6" />


## Arquitetura e Fluxo de Dados

O projeto é orquestrado pelo **Apache Airflow** e estruturado nas seguintes camadas:

### 🟤 Bronze (Raw)
- Ingestão de dados brutos do **MySQL**
- Persistência no **MinIO** em formato **Parquet**
- Inclusão de metadados de ingestão (`ingestion_date`)

---

### ⚪ Silver (Cleaned)
- **Saneamento Geográfico**
  - Consolidação da tabela de geolocalização
  - Um registro por CEP
  - Média para coordenadas (lat/lng) e moda para nomes de cidades
- **Normalização**
  - Limpeza de strings (remoção de acentos e caracteres especiais)
  - Tratamento de valores nulos (preenchimento por mediana em dimensões de produtos)
- **Feature Engineering**
  - Cálculo de SLA:
    - dias de entrega
    - dias de atraso
    - flag de atraso
  - Cálculo do volume dos produtos (`cm³`)

---

### 🟡 Gold (Curated)
- Modelagem **Star Schema** no **PostgreSQL (Data Warehouse)**
- Dimensões:
  - Calendário
  - Clientes
  - Produtos
  - Vendedores
- Tabelas Fato:
  - Vendas
  - Pagamentos
  - Reviews
  - Previsões Logísticas
- Camada pronta para **BI, Analytics e Machine Learning**

  
<img width="632" height="805" alt="Captura de tela 2026-01-19 170244" src="https://github.com/user-attachments/assets/e0bcbda8-1279-4830-807d-5fce96ad4118" />

---

## Stack Tecnológica

- **Linguagem:** Python  
  - Pandas  
  - SQLAlchemy  
  - Boto3  
  - Scikit-Learn  
- **Orquestração:** Apache Airflow  
- **Armazenamento de Objetos:** MinIO (S3 Compatible)  
- **Data Warehouse:** PostgreSQL  
- **Banco de Origem:** MySQL  
- **Infraestrutura:** Docker e Docker Compose  

---

## Machine Learning: Inteligência Logística

O pipeline integra um **script de inferência** que consome dados da camada **Silver** para prever o **risco de atraso** de pedidos ainda em trânsito (`shipped`, `processing`, `invoiced`). O modelo escolhido foi um Random Forest, treinado com ponderação automática para lidar com o desbalanceamento da variável alvo. Após o treino com lógica realista de handling time e otimização do limiar de decisão, foi definido um threshold ótimo de **0,5163**, equilibrando precisão e recall. O modelo alcançou acurácia global de **0,89** e, para a classe crítica (atraso), apresentou recall de **0,39**, precision de **0,27** e F1-score de **0,32**, demonstrando desempenho consistente como sistema de alerta antecipado, mesmo sob forte desbalanceamento da variável alvo.

### Engenharia de Atributos do Modelo

- **Distância Haversine**
  - Cálculo matemático da distância real entre vendedor e comprador (lat/lng)
- **Handling Time**
  - Tempo entre aprovação do pedido e entrega à transportadora
- **Riscos Calculados**
  - Risco histórico por **rota** (origem × destino)
  - Risco histórico por **categoria de produto**
- **Densidade Logística**
  - Relação peso / volume para identificação de perfis críticos

### Resultado do Modelo
- `probabilidade_atraso`
- `alerta_atraso` (baseado em threshold otimizado)

Os resultados são persistidos diretamente na **camada Gold**, prontos para consumo por **BI** ou aplicações operacionais.

---

##  Visualização de Dados (Power BI)

O dashboard consome as tabelas do **PostgreSQL** e está organizado em **quatro pilares analíticos**:

### 1. Perfomance Executiva
- GMV por ano e mês
- Ticket médio
- Taxa de atraso
- Quantidade total de pedidos
<img width="1535" height="854" alt="Captura de tela 2026-01-19 151302" src="https://github.com/user-attachments/assets/e8e2fac7-497f-4f98-82d8-f5862bf03cfa" />

### 2. Eficiência Logística
- Monitoramento de SLA por região
- Análise da taxa de atraso ao longo do tempo (ano e mês)
- Identificação de regiões com maior incidência de atrasos
- Avaliação do custo médio de frete por estado
- Análise do tempo médio de entrega por cidade
- Identificação de pedidos em risco operacional crítico
<img width="1532" height="853" alt="Captura de tela 2026-01-19 151027" src="https://github.com/user-attachments/assets/6e2d8685-ee91-43e5-a6c4-c4e4fb0316ee" />

### 3. Drivers de Receita
- Análise de Pareto (% acumulado) para identificação das categorias mais relevantes em faturamento
- Identificação de categorias estratégicas para crescimento e otimização comercial
- Cálculo e ranking de Lifetime Value (LTV) por cliente
<img width="1540" height="850" alt="Captura de tela 2026-01-19 151041" src="https://github.com/user-attachments/assets/51c4898e-0a40-432f-9d98-44a4235a5c9e" />

### 4. Satisfação do Cliente (Reviews)
- Cálculo da nota média geral dos clientes
- Distribuição da contagem de reviews por score (1 a 5)
- Análise da relação entre atraso na entrega e avaliação do cliente
- Correlação entre média de dias de atraso real e nota média
- Identificação de categorias de produto com pior experiência do cliente
- Exploração qualitativa de comentários de clientes para identificação de padrões de satisfação e insatisfação
<img width="1527" height="849" alt="Captura de tela 2026-01-19 151059" src="https://github.com/user-attachments/assets/496b5dee-d0b9-40ab-b4e5-ec231486d4c6" />

---

## Objetivo do Projeto

Demonstrar, de forma prática, uma **arquitetura moderna de dados**, integrando:
- Engenharia de Dados
- Modelagem Analítica
- Machine Learning
- Business Intelligence
<img width="1873" height="339" alt="Captura de tela 2026-01-19 153952" src="https://github.com/user-attachments/assets/19329209-71e9-43aa-9677-d9d7b231530f" />
