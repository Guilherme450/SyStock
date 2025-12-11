FROM python:3.11-slim

# Evita python .pyc e buffer do stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instala dependências do sistema (ex: gcc, libpq para Postgres)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Define o diretório base dentro do container
WORKDIR /app

# Copia apenas requirements primeiro (melhora cache)
COPY etl_pipeline/requirements.txt /app/requirements.txt

# Instala dependências do Python
RUN pip install --upgrade pip && pip install -r requirements.txt

# Agora copia o restante do código
COPY . /app

# Comando padrão (pode mudar depois)
CMD ["python", "-m", "etl_pipeline.orchestrator.main"]

