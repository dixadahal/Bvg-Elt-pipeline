import pytest
import psycopg2

# Database connection settings
DB_CONFIG = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432",
}

# Tables to check and their key columns
TABLES_TO_CHECK = {
    "staging.trips": "trip_id",
    "staging.stops": "stop_id",
    "staging.routes": "route_id",
    "staging.stop_times": "trip_id",
    "data_warehouse.station_dim": "station_id",
    "data_warehouse.route_dim": "route_id",
    "data_warehouse.date_dim": "date_id",
    "data_warehouse.ridership_fact": "fact_id",
}


@pytest.fixture(scope="module")
def db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    yield conn
    conn.close()


@pytest.mark.parametrize("table,key_col", TABLES_TO_CHECK.items())
def test_table_not_empty(db_connection, table, key_col):
    if table == "staging.stop_times":
        pytest.skip("staging.stop_times may be empty in staging")
    cur = db_connection.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    count = cur.fetchone()[0]
    cur.close()
    assert count > 0, f"{table} is empty!"


@pytest.mark.parametrize("table,key_col", TABLES_TO_CHECK.items())
def test_no_nulls(db_connection, table, key_col):
    cur = db_connection.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {key_col} IS NULL;")
    null_count = cur.fetchone()[0]
    cur.close()
    assert null_count == 0, f"{table} has {null_count} NULL {key_col} values!"


@pytest.mark.parametrize("table,key_col", TABLES_TO_CHECK.items())
def test_no_duplicates(db_connection, table, key_col):
    if table == "staging.stop_times":
        pytest.skip("Duplicates allowed in staging.stop_times")
    cur = db_connection.cursor()
    cur.execute(
        f"""
        SELECT COUNT(*) 
        FROM (
            SELECT {key_col}, COUNT(*) 
            FROM {table} 
            GROUP BY {key_col} 
            HAVING COUNT(*) > 1
        ) dup;
    """
    )
    dup_count = cur.fetchone()[0]
    cur.close()
    assert dup_count == 0, f"{table} has {dup_count} duplicate {key_col} values!"
