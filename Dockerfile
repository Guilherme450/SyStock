FROM python:3.11-slim

# Evita python .pyc e buffer do stdout
# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1

# Variáveis de ambiente 
# recomendadas para Prefect 
ENV PREFECT_API_URL=http://prefect-server:4200/api 
ENV PREFECT_LOGGING_LEVEL=INFO \
     PYTHONDONTWRITEBYTECODE=1 \ 
     PYTHONUNBUFFERED=1

# Instala dependências do sistema (ex: gcc, libpq para Postgres)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Define o diretório base dentro do container
WORKDIR /app

# Copia apenas requirements primeiro (melhora cache)
COPY etl_pipeline/requirements.txt /app/requirements.txt

# Instala dependências do Python
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copia o restante do código para a imagem (não montamos o código por padrão)
COPY . /app

# Garante que o diretório /app esteja no PYTHONPATH para imports de pacote funcionarem
ENV PYTHONPATH=/app

# Porta opcional (caso o pipeline exponha algo no futuro)
# EXPOSE 8080

# Comando padrão: executa o pipeline como módulo
CMD ["python", "-m", "pipeline_manager.pipeline_flow"]

