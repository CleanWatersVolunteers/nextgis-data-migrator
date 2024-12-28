from datetime import datetime
import requests
import psycopg2
import os

# URLs for data sources
POLLUTIONS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/60/geojson"
BIRDS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/100/geojson"
PICKUP_POINTS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/98/geojson"

# Database configuration
db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

def fetch_json_data(url):
    """Fetch JSON data from the provided URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def process_features(data, table_name, process_row, print_label):
    """
    Generic method to process features for a dataset.

    :param data: The JSON data from the API.
    :param table_name: The target table name.
    :param process_row: A function defining how to process each row.
    :param print_label: The label for the dataset.
    """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    features = data.get("features", [])
    print(f"--- {print_label} ---")

    for feature in features:
        properties = feature.get("properties", {})
        upsert_query, values = process_row(properties)
        cursor.execute(upsert_query, values)

    conn.commit()
    print(f"Total {print_label} features processed: {len(features)}\n")

    cursor.close()
    conn.close()

def pollution_row(properties):
    """Processes a single row for the pollution table."""
    latitude = properties.get('lat')
    longitude = properties.get('lon')
    comment = properties.get('comment')
    status = properties.get('status_us') or "Unknown"
    info_source = properties.get('source')
    discovered_at = properties.get('dt_auto') or datetime.now()
    surface_type = properties.get('type_surf')

    upsert_query = """
    INSERT INTO pollution (longitude, latitude, comment, status, info_source, 
                           discovered_at, surface_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (latitude, longitude) 
    DO UPDATE SET
        comment = EXCLUDED.comment,
        status = EXCLUDED.status,
        info_source = EXCLUDED.info_source,
        surface_type = EXCLUDED.surface_type,
        updated_at = now();
    """
    values = (longitude, latitude, comment, status, info_source, discovered_at, surface_type)
    return upsert_query, values

def bird_row(properties):
    """Processes a single row for the bird table."""
    latitude = properties.get('lat')
    longitude = properties.get('lon')
    status = properties.get('status_us') or "Unknown"
    info_source = properties.get('source')
    discovered_at = properties.get('dt_auto') or None
    priority = properties.get('priority')
    comment = properties.get('comment')

    upsert_query = """
    INSERT INTO bird (longitude, latitude, status, info_source, 
                      discovered_at, priority, comment)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (latitude, longitude) 
    DO UPDATE SET
        status = EXCLUDED.status,
        info_source = EXCLUDED.info_source,
        priority = EXCLUDED.priority,
        comment = EXCLUDED.comment,
        updated_at = now();
    """
    values = (longitude, latitude, status, info_source, discovered_at, priority, comment)
    return upsert_query, values

def pickup_point_row(properties):
    """Processes a single row for the pick-up point table."""
    latitude = properties.get('lat')
    longitude = properties.get('lon')
    comment = properties.get('comment')
    status = properties.get('status_us') or "Unknown"
    info_source = properties.get('source')
    discovered_at = properties.get('dt_auto') or datetime.now()

    upsert_query = """
    INSERT INTO pick_up_point (longitude, latitude, comment, status, info_source, 
                               discovered_at)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (latitude, longitude) 
    DO UPDATE SET
        comment = EXCLUDED.comment,
        status = EXCLUDED.status,
        info_source = EXCLUDED.info_source,
        updated_at = now();
    """
    values = (longitude, latitude, comment, status, info_source, discovered_at)
    return upsert_query, values

def main():
    datasets = [
        (POLLUTIONS_URL, "pollution", pollution_row, "Загрязнения"),
        (BIRDS_URL, "bird", bird_row, "Птицы"),
        (PICKUP_POINTS_URL, "pick_up_point", pickup_point_row, "Точки вывоза"),
    ]

    for url, table_name, row_processor, print_label in datasets:
        data = fetch_json_data(url)
        if data:
            process_features(data, table_name, row_processor, print_label)

if __name__ == "__main__":
    main()
