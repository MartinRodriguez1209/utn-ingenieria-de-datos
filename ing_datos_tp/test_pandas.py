from dotenv import load_dotenv
import os
import requests
import pandas as pd

load_dotenv()

api_key = os.getenv("NASA_API_KEY")

# ─────────────────────────────────────────
# ENDPOINT 1: Feed diario (incremental)
# ─────────────────────────────────────────
print("Consultando Endpoint 1: Feed diario...")

url_feed = "https://api.nasa.gov/neo/rest/v1/feed"

params_feed = {"start_date": "2026-05-27", "end_date": "2026-06-01", "api_key": api_key}

response_feed = requests.get(url_feed, params=params_feed)
data_feed = response_feed.json()

print(f"Status code: {response_feed.status_code}")
print(f"Total asteroides: {data_feed['element_count']}")

# Construir DataFrame de aproximaciones
aproximaciones = []

for fecha, asteroides in data_feed["near_earth_objects"].items():
    for asteroide in asteroides:
        close_approach = asteroide["close_approach_data"][0]
        aproximaciones.append(
            {
                "id": asteroide["id"],
                "name": asteroide["name"],
                "close_approach_date": close_approach["close_approach_date"],
                "close_approach_date_full": close_approach["close_approach_date_full"],
                "relative_velocity_km_per_second": close_approach["relative_velocity"][
                    "kilometers_per_second"
                ],
                "miss_distance_km": close_approach["miss_distance"]["kilometers"],
                "orbiting_body": close_approach["orbiting_body"],
                "is_potentially_hazardous": asteroide[
                    "is_potentially_hazardous_asteroid"
                ],
            }
        )

df_aproximaciones = pd.DataFrame(aproximaciones)

print("\n--- DataFrame Aproximaciones (incremental) ---")
pd.set_option("display.max_columns", None)  # muestra todas las columnas
pd.set_option("display.width", None)  # no trunca el ancho
pd.set_option("display.max_colwidth", None)  # no trunca el contenido de cada celda
print(df_aproximaciones)
