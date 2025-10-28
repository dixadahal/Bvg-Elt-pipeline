import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def data_quality_check():
    conn = psycopg2.connect(
        dbname="airflow", user="airflow", password="airflow", host="postgres"
    )
    cur = conn.cursor()

    tables_to_check = {
        "staging.trips": "trip_id",
        "staging.stops": "stop_id",
        "staging.routes": "route_id",
        "staging.stop_times": "trip_id",
        "data_warehouse.station_dim": "station_id",
        "data_warehouse.route_dim": "route_id",
        "data_warehouse.date_dim": "date_id",
        "data_warehouse.ridership_fact": "fact_id",
    }

    for table, key_col in tables_to_check.items():
        logging.info(f"\nChecking table: {table}")

        # Row count
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        row_count = cur.fetchone()[0]
        logging.info(f"Row count: {row_count}")

        # Duplicates
        cur.execute(
            f"""
            SELECT {key_col}, COUNT(*) 
            FROM {table} 
            GROUP BY {key_col} 
            HAVING COUNT(*) > 1;
        """
        )
        duplicates = cur.fetchall()
        if duplicates:
            logging.warning(
                f"Found {len(duplicates)} duplicate {key_col}(s) in {table}. Showing first 5:"
            )
            for dup in duplicates[:5]:
                logging.warning(dup)

        # Nulls
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {key_col} IS NULL;")
        null_count = cur.fetchone()[0]
        if null_count > 0:
            logging.warning(f"Found {null_count} null {key_col}(s) in {table}!")

    cur.close()
    conn.close()
    logging.info("✅ All data quality checks completed!")
