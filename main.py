import requests
import psycopg2

POLLUTIONS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/60/geojson"
BIRDS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/100/geojson"
PICKUP_POINTS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/98/geojson"

# Database configuration
db_config = {
    "dbname": "clean-waters",
    "user": "postgres",
    "password": "postgres",
    "host": "109.172.6.123",
    "port": 5441
}

def fetch_and_process_data(url, data_type):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        features = data.get("features", [])
        print(f"--- {data_type} ---")
        for feature in features:
            properties = feature.get("properties", {})
            latitude = properties.get('lat')
            longitude = properties.get('lon')
            comment = properties.get('comment')
            status = properties.get('status_us')
            info_source = properties.get('source')
            discovered_at = properties.get('dt_auto')
            surface_type = properties.get('type_surf')

            # Construct the UPSERT query
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

            # Execute the query
            cursor.execute(upsert_query, (longitude, latitude, comment, status, info_source,
                                          discovered_at, surface_type))

        # Commit the transaction and close connection
        conn.commit()
        print(f"Total {data_type} features processed: {len(features)}\n")

    except requests.exceptions.RequestException as e:
        print(f"Error processing {data_type}: {e}")
    except Exception as e:
        print(f"General error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    urls = [
        (POLLUTIONS_URL, "Загрязнения"),
        # (BIRDS_URL, "Птицы"),
        # (PICKUP_POINTS_URL, "Точки вывоза")
    ]

    for url, data_type in urls:
        print(f"Processing {data_type}...")
        fetch_and_process_data(url, data_type)

if __name__ == "__main__":
    main()