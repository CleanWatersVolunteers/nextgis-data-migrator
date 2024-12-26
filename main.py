import requests

POLLUTIONS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/60/geojson"
BIRDS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/100/geojson"
PICKUP_POINTS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/98/geojson"

def fetch_and_process_data(url, data_type):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        print(f"--- {data_type} ---")
        for feature in features:
            properties = feature.get("properties", {})
            print(properties)

        print(f"Total {data_type} features processed: {len(features)}\n")
    except requests.exceptions.RequestException as e:
        print(f"Error processing {data_type}: {e}")

def main():
    urls = [
        (POLLUTIONS_URL, "Загрязнения"),
        (BIRDS_URL, "Птицы"),
        (PICKUP_POINTS_URL, "Точки вывоза")
    ]

    for url, data_type in urls:
        print(f"Processing {data_type}...")
        fetch_and_process_data(url, data_type)

if __name__ == "__main__":
    main()
