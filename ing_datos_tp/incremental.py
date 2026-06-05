import pandas as pd


def extraer_aproximaciones(data):
    """
    Construye el DataFrame de aproximaciones a partir de los datos crudos del feed.
    Recibe el JSON de respuesta del endpoint /neo/rest/v1/feed.
    Devuelve un DataFrame con los datos de aproximación de cada asteroide,
    particionables por fecha para la carga incremental.
    """
    aproximaciones = []

    for fecha, asteroides in data["near_earth_objects"].items():
        for asteroide in asteroides:
            close_approach = asteroide["close_approach_data"][0]
            aproximaciones.append(
                {
                    "id": asteroide["id"],
                    "name": asteroide["name"],
                    "close_approach_date": close_approach["close_approach_date"],
                    "close_approach_date_full": close_approach[
                        "close_approach_date_full"
                    ],
                    "relative_velocity_km_per_second": close_approach[
                        "relative_velocity"
                    ]["kilometers_per_second"],
                    "miss_distance_km": close_approach["miss_distance"]["kilometers"],
                    "orbiting_body": close_approach["orbiting_body"],
                    "is_potentially_hazardous": asteroide[
                        "is_potentially_hazardous_asteroid"
                    ],
                }
            )

    df = pd.DataFrame(aproximaciones)

    # Conversión de tipos de datos
    df["close_approach_date"] = pd.to_datetime(df["close_approach_date"])
    df["close_approach_date_full"] = pd.to_datetime(df["close_approach_date_full"])
    df["relative_velocity_km_per_second"] = df[
        "relative_velocity_km_per_second"
    ].astype(float)
    df["miss_distance_km"] = df["miss_distance_km"].astype(float)

    return df
