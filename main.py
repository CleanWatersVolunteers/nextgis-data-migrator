import requests

POLLUTIONS_URL = "https://blacksea-monitoring.nextgis.com/api/resource/60/geojson"

def fetch_and_process_data():
    try:
        response = requests.get(POLLUTIONS_URL)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        for feature in features:
            properties = feature.get("properties", {})
            print(properties)

        print(f"Total features processed: {len(features)}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {e}")

if __name__ == "__main__":
    fetch_and_process_data()
