FROM apache/airflow:2.9.0

    # Copia o arquivo de requisitos para dentro da imagem
    COPY airflow/requirements.txt /requirements.txt

    # Instala as dependências
    RUN pip install --no-cache-dir -r /requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.12.txt"
