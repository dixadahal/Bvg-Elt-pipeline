from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

# Create station_dim and load data from staging.stops
engine.execute("""
CREATE TABLE IF NOT EXISTS data_warehouse.station_dim (
    station_id TEXT PRIMARY KEY,
    stop_name TEXT,
    stop_lat FLOAT,
    stop_lon FLOAT
);
INSERT INTO data_warehouse.station_dim (station_id, stop_name, stop_lat, stop_lon)
SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon
FROM staging.stops
ON CONFLICT (station_id) DO NOTHING;
""")
# Create route_dim and load data from staging.routes
engine.execute("""
CREATE TABLE IF NOT EXISTS data_warehouse.route_dim (
    route_id TEXT PRIMARY KEY,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INT
);
INSERT INTO data_warehouse.route_dim (route_id, route_short_name, route_long_name, route_type)
SELECT DISTINCT route_id, route_short_name, route_long_name, route_type
FROM staging.routes
ON CONFLICT (route_id) DO NOTHING;
""")
# Corrected date_dim code
engine.execute("""
CREATE TABLE IF NOT EXISTS data_warehouse.date_dim (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT
);
INSERT INTO data_warehouse.date_dim (date_id, year, month, day, day_of_week)
SELECT TO_DATE(date::TEXT, 'YYYYMMDD') AS date_id,
       EXTRACT(YEAR FROM TO_DATE(date::TEXT, 'YYYYMMDD')),
       EXTRACT(MONTH FROM TO_DATE(date::TEXT, 'YYYYMMDD')),
       EXTRACT(DAY FROM TO_DATE(date::TEXT, 'YYYYMMDD')),
       EXTRACT(DOW FROM TO_DATE(date::TEXT, 'YYYYMMDD'))
FROM staging.calendar_dates
ON CONFLICT (date_id) DO NOTHING;
""")
# Create ridership_fact and populate it
engine.execute("""
CREATE TABLE IF NOT EXISTS data_warehouse.ridership_fact (
    fact_id SERIAL PRIMARY KEY,
    date_id DATE REFERENCES data_warehouse.date_dim(date_id),
    station_id TEXT REFERENCES data_warehouse.station_dim(station_id),
    route_id TEXT REFERENCES data_warehouse.route_dim(route_id),
    trip_id TEXT,
    passenger_count INT
);

INSERT INTO data_warehouse.ridership_fact (date_id, station_id, route_id, trip_id, passenger_count)
SELECT
    TO_DATE(cd.date::TEXT, 'YYYYMMDD') AS date_id,
    st.stop_id AS station_id,
    r.route_id,
    t.trip_id,
    FLOOR(RANDOM() * 100)::INT AS passenger_count  -- simulate passengers
FROM staging.trips t
JOIN staging.stop_times stt ON t.trip_id = stt.trip_id
JOIN staging.stops st ON stt.stop_id = st.stop_id
JOIN staging.routes r ON t.route_id = r.route_id
JOIN staging.calendar_dates cd ON cd.service_id = t.service_id;
""")
