import requests
import psycopg2

# URLs for data sources
POLLUTIONS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/60/geojson"
BIRDS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/100/geojson"

# Database configuration
db_config = {
    "dbname": "clean-waters",
    "user": "postgres",
    "password": "postgres",
    "host": "109.172.6.123",
    "port": 5441
}

def fetch_and_process_pollution_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        features = data.get("features", [])
        print("--- Загрязнения ---")
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

        conn.commit()
        print(f"Total Загрязнения features processed: {len(features)}\n")

    except requests.exceptions.RequestException as e:
        print(f"Error processing Загрязнения: {e}")
    except Exception as e:
        print(f"General error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def fetch_and_process_bird_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        features = data.get("features", [])
        print("--- Птицы ---")
        for feature in features:
            properties = feature.get("properties", {})
            latitude = properties.get('lat')
            longitude = properties.get('lon')
            status = properties.get('status_us') or "Unknown"
            info_source = properties.get('source')
            discovered_at = properties.get('dt_auto')
            priority = properties.get('priority')
            captured = "captured" in (properties.get('comment', "").lower())

            # Construct the UPSERT query
            upsert_query = """
            INSERT INTO bird (longitude, latitude, status, info_source, 
                              discovered_at, priority, captured)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (latitude, longitude) 
            DO UPDATE SET
                status = EXCLUDED.status,
                info_source = EXCLUDED.info_source,
                priority = EXCLUDED.priority,
                captured = EXCLUDED.captured,
                updated_at = now();
            """

            # Execute the query
            cursor.execute(upsert_query, (longitude, latitude, status, info_source,
                                          discovered_at, priority, captured))

        conn.commit()
        print(f"Total Птицы features processed: {len(features)}\n")

    except requests.exceptions.RequestException as e:
        print(f"Error processing Птицы: {e}")
    except Exception as e:
        print(f"General error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    urls = [
        (POLLUTIONS_URL, fetch_and_process_pollution_data),
        (BIRDS_URL, fetch_and_process_bird_data),
    ]

    for url, handler in urls:
        print(f"Processing {handler.__name__.split('_')[-1]}...")
        handler(url)

if __name__ == "__main__":
    main()
