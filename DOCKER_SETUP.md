# SyStock - Data Warehouse com Docker

## Configuração do Ambiente

### Pré-requisitos
- Docker (versão 20.10+)
- Docker Compose (versão 1.29+)
- Git

### Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com base no `.env.example`:

```bash
cp .env.example .env
```

Edite o arquivo `.env` com suas configurações:

```env
# Database Configuration
DB_HOST=postgres-dw
DB_PORT=5432
DB_NAME=systock_dw
DB_USER=postgres
DB_PASSWORD=postgres  # ALTERE PARA UM PASSWORD SEGURO EM PRODUÇÃO

# PgAdmin Configuration
PGADMIN_EMAIL=admin@example.com
PGADMIN_PASSWORD=admin

# ETL Configuration
ETL_LOG_LEVEL=INFO

# Prefect Configuration
PREFECT_API_URL=http://prefect:4200/api

# Application Configuration
ENVIRONMENT=development
DEBUG=true
```

## Iniciando os Serviços

### 1. Iniciar todos os containers

```bash
docker-compose up -d
```

### 2. Verificar status dos containers

```bash
docker-compose ps
```

### 3. Acessar os serviços

- **PostgreSQL Data Warehouse**: `localhost:5432`
  - Usuário: postgres
  - Senha: (conforme definido no .env)
  - Database: systock_dw

- **PgAdmin**: http://localhost:5050
  - Email: admin@example.com
  - Senha: admin

- **Prefect Server**: http://localhost:4200
  - Interface para orquestração de workflows

## Estrutura da Data Warehouse

O banco de dados é organizado em 4 schemas (camadas):

### 1. **Bronze Layer** (`bronze` schema)
- Armazena dados brutos extraídos do OLTP
- Sem transformações
- Replicação 1:1 das tabelas de origem

### 2. **Silver Layer** (`silver` schema)
- Dados limpos e normalizados
- Aplicação de regras de negócio básicas
- Remoção de duplicatas e valores nulos

### 3. **Gold Layer** (`gold` schema)
- Dados agregados e prontos para análise
- Tabelas dimensionais (dim_*) e fatos (fact_*)
- Tabela de log de execução de pipelines

### 4. **Staging** (`staging` schema)
- Tabelas temporárias para processamento
- Limpas entre execuções de pipeline

## Gerenciamento da Data Warehouse

### Conectar ao PostgreSQL

#### Usando psql (linha de comando)
```bash
docker exec -it systock-postgres-dw psql -U postgres -d systock_dw
```

#### Usando PgAdmin (Interface Web)
1. Acesse http://localhost:5050
2. Login com as credenciais do `.env`
3. Clique em "Add New Server"
4. Configure:
   - **Name**: SyStock DW
   - **Hostname/address**: postgres-dw
   - **Port**: 5432
   - **Username**: postgres
   - **Password**: (conforme .env)

### Executar Scripts SQL

```bash
docker exec -i systock-postgres-dw psql -U postgres -d systock_dw < script.sql
```

### Ver logs do PostgreSQL

```bash
docker logs systock-postgres-dw
```

## Execução do ETL Pipeline

### Iniciar o pipeline manualmente

```bash
docker exec systock-etl python -m orchestrator.prefect_flow.main
```

### Ver logs do ETL

```bash
docker logs -f systock-etl
```

## Parando os Serviços

### Parar todos os containers (mantém dados)

```bash
docker-compose stop
```

### Remover todos os containers

```bash
docker-compose down
```

### Remover todos os containers e volumes (CUIDADO - apaga dados!)

```bash
docker-compose down -v
```

## Troubleshooting

### PostgreSQL não inicia

```bash
# Verificar logs
docker logs systock-postgres-dw

# Verificar volume
docker volume ls | grep postgres

# Remover e recriar (cuidado com dados)
docker-compose down -v
docker-compose up -d postgres-dw
```

### ETL pipeline não consegue conectar ao banco

```bash
# Verificar conectividade
docker exec systock-etl pg_isready -h postgres-dw -p 5432 -U postgres

# Verificar variáveis de ambiente
docker exec systock-etl env | grep DB_
```

### Prefect não inicia

```bash
# Limpar e reconstruir
docker-compose stop prefect
docker-compose rm prefect
docker-compose up -d prefect
```

## Desenvolvimento

### Compilar Dockerfile com flag de desenvolvimento

```bash
docker-compose build --no-cache etl-pipeline
```

### Atualizar requirements.txt após mudanças

```bash
docker-compose build etl-pipeline
docker-compose up -d etl-pipeline
```

### Adicionar novos volumes ou envs

Edite o `docker-compose.yml` e execute:
```bash
docker-compose up -d --force-recreate
```

## Segurança em Produção

⚠️ **IMPORTANTE**: Altere as credenciais padrão antes de colocar em produção:

1. Altere `DB_PASSWORD` para uma senha forte
2. Altere `PGADMIN_PASSWORD` para uma senha forte
3. Configure variáveis de ambiente seguras
4. Use secrets do Docker em ambientes de produção
5. Configure backups automáticos do PostgreSQL
6. Implemente SSL/TLS para conexões ao banco

## Recursos Adicionais

- [Documentação Docker](https://docs.docker.com/)
- [Documentação PostgreSQL](https://www.postgresql.org/docs/)
- [Documentação Prefect](https://docs.prefect.io/)
- [Documentação PgAdmin](https://www.pgadmin.org/docs/)
