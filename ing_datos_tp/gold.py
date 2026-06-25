import pandas as pd


def calcular_resumen_diario(df_aproximaciones, df_metadatos):
    """
    cruza aproximaciones con metadatos y agrupa por fecha para obtener
    estadisticas diarias de actividad de asteroides
    """
    df_completo = pd.merge(df_aproximaciones, df_metadatos, on="id", how="left")

    # bajo_seguimiento_riesgo puede llegar con nulos para las aproximaciones
    # sin metadato asociado (los huerfanos ya conocidos). se castea a float
    # antes de sumar para que el resultado de la agregacion sea siempre
    # numerico y consistente, en vez de mezclar bool/nan/object segun el grupo
    df_completo["bajo_seguimiento_riesgo"] = df_completo[
        "bajo_seguimiento_riesgo"
    ].astype(float)

    df_resumen = (
        df_completo.groupby("fecha")
        .agg(
            cantidad_aproximaciones=("id", "count"),
            velocidad_promedio_km_s=("relative_velocity_km_per_second", "mean"),
            velocidad_maxima_km_s=("relative_velocity_km_per_second", "max"),
            distancia_minima_km=("miss_distance_km", "min"),
            distancia_promedio_km=("miss_distance_km", "mean"),
            cantidad_bajo_seguimiento_riesgo=("bajo_seguimiento_riesgo", "sum"),
            diametro_maximo_km=("diameter_max_km", "max"),
        )
        .reset_index()
    )

    columnas_a_redondear = [
        "velocidad_promedio_km_s",
        "velocidad_maxima_km_s",
        "distancia_minima_km",
        "distancia_promedio_km",
        "diametro_maximo_km",
    ]
    df_resumen[columnas_a_redondear] = df_resumen[columnas_a_redondear].round(2)

    return df_resumen
