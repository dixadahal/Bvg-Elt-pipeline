# **BVG ELT Pipeline Project**

## **Overview**
This project builds an ELT pipeline for BVG (Berlin Public Transport) GTFS data using Docker, PostgreSQL, and Apache Airflow.
The goal is to ingest, process, and analyse public transport data efficiently and make it available for dashboards and reporting.
## **Setup**
### Prerequisites:
- Docker & Docker Compose installed
- Git installed

## **Steps**:
1.Clone the repository:
-git clone https://github.com/<username>/bvg-elt.git
-cd bvg-elt
2.Copy the environment file and set your values:
-copy .env.example .env
3.Open .env in a text editor and check values (Postgres, Airflow keys, Metabase).
4.Start the containers:
-docker compose up -d 

## **Access services**:
Airflow Web UI: http://localhost:8080
PostgreSQL: localhost:5432
Metabase: http://localhost:3000

## **Environment Variables**
The `.env.example` file contains all required variables. Copy it to `.env` and update values if needed:

| Variable | Purpose |
|----------|---------|
| POSTGRES_USER | Postgres username |
| POSTGRES_PASSWORD | Postgres password |
| POSTGRES_DB | Postgres database name |
| POSTGRES_PORT | Postgres port |
| AIRFLOW__CORE__FERNET_KEY | Airflow encryption key |
| AIRFLOW__WEBSERVER__SECRET_KEY | Airflow web UI session key |
| AIRFLOW__CORE__LOAD_EXAMPLES | Load example DAGs (False for clean setup) |
| AIRFLOW_ADMIN_USER | Airflow web UI username |
| AIRFLOW_ADMIN_PASSWORD | Airflow web UI password |
| MB_DB_FILE | Metabase database file path |

## **Dataset**
The project uses GTFS data from OpenMobilityData
Data files (.txt) are stored in data_lake/landing.
Example files: stops.txt, routes.txt, trips.txt, stop_times.txt.

## **Usage**
- Airflow DAGs orchestrate:
-Ingestion → download GTFS ZIP into data_lake/landing
-Transformation → clean & stage data into PostgreSQL
-Loading → move processed data into warehouse schema
-Users can query PostgreSQL or build dashboards in Metabase/Grafana.
## **Repository Layout**
bvg-elt/
├── dags/                # Airflow DAGs
├── data_lake/           # Landing, staging, warehouse data folders
├── logs/                # Airflow logs
├── metabase-data/       # Metabase database file
├── tests/               # Test scripts
├── docker-compose.yml
├── .env.example
├── .env
└── README.md

## **ELT Flow Diagram**
```mermaid
flowchart LR
    A[Landing Zone<br/>Raw GTFS Files] --> B[Staging Schema<br/>Postgres]
    B --> C[Data Warehouse<br/>Analytics Schema]
    C --> D[Dashboards & Reports<br/>Metabase/Grafana]


