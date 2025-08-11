# BVG ELT Pipeline Project

## Overview
This project builds an ELT pipeline for BVG (Berlin Public Transport) GTFS data using Docker, PostgreSQL, and Apache Airflow.  
The goal is to ingest, process, and analyze public transport data efficiently.

## Setup
- Docker and Docker Compose installed  
- PostgreSQL and Airflow containers set up and running  
- Dataset files stored in the `data_lake/landing` directory

## Dataset
The project uses GTFS data from OpenMobilityData.  
Files are stored as `.txt` in the landing of the data lake.

## Usage
- Run Docker containers for PostgreSQL and Airflow  
- Use Airflow DAGs to schedule data ingestion and processing  
- Query data in PostgreSQL for analysis

## Next Steps
- Develop Python scripts for data ingestion from landing to raw/staging schemas  
- Schedule ingestion workflows in Airflow DAGs  
- Implement ELT transformations and load processed data for analytics

## Project Structure
- `dags/` - Airflow DAG files  
- `data_lake/` - Raw data storage  
- `scripts/` - Python ingestion and transformation scripts  
- `docker-compose.yml` - Docker setup for containers


