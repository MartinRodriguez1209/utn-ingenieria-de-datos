import requests
import pandas as pd


def extraer_metadatos(ids, api_key):
    """
    Extrae los metadatos completos de cada asteroide via el endpoint de lookup.
    Recibe una lista de IDs y la API key de NASA.
    Devuelve un DataFrame con los metadatos estáticos de cada asteroide.
    Los asteroides que fallen se omiten con un mensaje de advertencia.
    """
    print("Extrayendo metadatos de cada asteroide...")

    metadatos = []

    for asteroide_id in ids:
        url = f"https://api.nasa.gov/neo/rest/v1/neo/{asteroide_id}"

        try:
            response = requests.get(url, params={"api_key": api_key}, timeout=10)
            response.raise_for_status()
            data = response.json()

            metadatos.append(
                {
                    "id": data["id"],
                    "name": data["name"],
                    "absolute_magnitude_h": data["absolute_magnitude_h"],
                    "diameter_min_km": data["estimated_diameter"]["kilometers"][
                        "estimated_diameter_min"
                    ],
                    "diameter_max_km": data["estimated_diameter"]["kilometers"][
                        "estimated_diameter_max"
                    ],
                    "is_potentially_hazardous": data[
                        "is_potentially_hazardous_asteroid"
                    ],
                    "is_sentry_object": data["is_sentry_object"],
                    "nasa_jpl_url": data["nasa_jpl_url"],
                }
            )

            print(f"  Metadatos obtenidos: {data['name']}")

        except requests.exceptions.ConnectionError:
            print(
                f"  Advertencia: no se pudo conectar para el asteroide ID {asteroide_id}. Se omite."
            )
        except requests.exceptions.Timeout:
            print(
                f"  Advertencia: timeout para el asteroide ID {asteroide_id}. Se omite."
            )
        except requests.exceptions.HTTPError as e:
            print(
                f"  Advertencia: error HTTP para el asteroide ID {asteroide_id}: {e}. Se omite."
            )

    df = pd.DataFrame(metadatos)

    # Conversión de tipos
    df["absolute_magnitude_h"] = df["absolute_magnitude_h"].astype(float)
    df["diameter_min_km"] = df["diameter_min_km"].astype(float)
    df["diameter_max_km"] = df["diameter_max_km"].astype(float)
    df["is_potentially_hazardous"] = df["is_potentially_hazardous"].astype(bool)
    df["is_sentry_object"] = df["is_sentry_object"].astype(bool)

    return df
