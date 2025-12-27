# SyStock — Data Warehouse with Docker

## Environment setup

### Requirements
- Docker (20.10+)
- Docker Compose (1.29+)
- Git

### Environment variables

Create a `.env` file at the repository root from the provided example:

```bash
cp .env.example .env
```

Edit `.env` with your configuration:

```env
# Database
DB_HOST=postgres-dw
DB_PORT=5432
DB_NAME=systock_dw
DB_USER=postgres
DB_PASSWORD=postgres

# PgAdmin
PGADMIN_EMAIL=admin@example.com
PGADMIN_PASSWORD=admin

# ETL
ETL_LOG_LEVEL=INFO

# Prefect
PREFECT_API_URL=http://prefect:4200/api

# App
ENVIRONMENT=development
DEBUG=true
```

## Start services

Start all containers:

```bash
docker-compose up -d
```

Check container status:

```bash
docker-compose ps
```

Access services:

- PostgreSQL: `localhost:5432` (user `postgres`, database `systock_dw`)
- PgAdmin: http://localhost:5050 (use credentials from `.env`)
- Prefect: http://localhost:4200

## Warehouse layers

The warehouse uses four logical schemas:

- Bronze (`bronze`) — raw ingested data
- Silver (`silver`) — cleaned and normalized data
- Gold (`gold`) — aggregated, analysis-ready tables (dimensions and facts)
- Staging (`staging`) — transient tables for processing

## Manage the data warehouse

Connect to PostgreSQL CLI:

```bash
docker exec -it systock-postgres-dw psql -U postgres -d systock_dw
```

Run SQL scripts:

```bash
docker exec -i systock-postgres-dw psql -U postgres -d systock_dw < script.sql
```

View PostgreSQL logs:

```bash
docker logs systock-postgres-dw
```

Start the ETL pipeline manually:

```bash
docker exec systock-etl python -m orchestrator.prefect_flow.main
```

View ETL logs:

```bash
docker logs -f systock-etl
```

Stop containers (preserve data):

```bash
docker-compose stop
```

Bring down containers:

```bash
docker-compose down
```

Bring down containers and volumes (removes data):

```bash
docker-compose down -v
```

## Troubleshooting

PostgreSQL won't start — check logs and volume state:

```bash
docker logs systock-postgres-dw
docker volume ls | grep postgres
```

ETL cannot connect to DB — verify connectivity and env vars:

```bash
docker exec systock-etl pg_isready -h postgres-dw -p 5432 -U postgres
docker exec systock-etl env | grep DB_
```

Prefect issues — recreate the service:

```bash
docker-compose stop prefect
docker-compose rm prefect
docker-compose up -d prefect
```

## Development

Build ETL image (development):

```bash
docker-compose build --no-cache etl-pipeline
```

Rebuild after `requirements.txt` changes:

```bash
docker-compose build etl-pipeline
docker-compose up -d etl-pipeline
```

Add volumes or env vars by editing `docker-compose.yml` and recreating services:

```bash
docker-compose up -d --force-recreate
```

## Production notes

Before deploying to production:

1. Replace default passwords in `.env`
2. Use Docker secrets for sensitive values
3. Configure automated PostgreSQL backups
4. Enable SSL/TLS for DB connections

## Resources

- https://docs.docker.com/
- https://www.postgresql.org/docs/
- https://docs.prefect.io/
- https://www.pgadmin.org/docs/
