FROM apache/airflow:2.9.1

# Instala dependências do sistema operacional (se necessário)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean

# Volta para o usuário airflow
USER airflow

# COPIA o requirements.txt da sua máquina para dentro da imagem
COPY requirements.txt /requirements.txt

# Instala as bibliotecas lendo o arquivo
RUN pip install --no-cache-dir -r /requirements.txt