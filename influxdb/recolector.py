import requests
import schedule
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os 

# --- CONFIGURACIÓN ---
METEO_TOKEN = os.getenv("METEO_TOKEN")
VARIABLES = "temperature,wind,precipitation_amount,relative_humidity,sky_state,significative_wave_height"

# Red de puntos representativos de Galicia (Añade o quita los que quieras)
UBICACIONES = {
    "A Coruña": ("-8.41", "43.36"),
    "Vigo": ("-8.72", "42.24"),
    "Santiago": ("-8.54", "42.87"),
    "Lugo": ("-7.55", "43.01"),
    "Ourense": ("-7.86", "42.33"),
    "Pontevedra": ("-8.64", "42.43"),
    "Ferrol": ("-8.23", "43.48"),
    "Costa da Morte (Fisterra)": ("-9.26", "42.90"),
    "A Mariña (Ribadeo)": ("-7.04", "43.53"),
    "Montaña (Manzaneda)": ("-7.25", "42.25")
}

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
ORG = "galicia_data"
BUCKET = "meteo_bucket"

def obtener_y_guardar_datos():
    print(f"[{datetime.now()}] Iniciando barrido de MeteoSIX v5 para TODA Galicia...")

    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        for ciudad, (lon, lat) in UBICACIONES.items():
            print(f"Descargando datos de: {ciudad}...")
            API_URL = f"https://servizos.meteogalicia.gal/apiv5/getNumericForecastInfo?coords={lon},{lat}&variables={VARIABLES}&API_KEY={METEO_TOKEN}"

            response = requests.get(API_URL)
            response.raise_for_status()
            data = response.json()

            puntos = []
            
            # BLINDAJE CONTRA NULLS (None) DE LA API
            features = data.get("features") or []
            for feature in features:
                propiedades = feature.get("properties") or {}

                days = propiedades.get("days") or []
                for day in days:
                    variables = day.get("variables") or []
                    for variable in variables:
                        var_name = variable.get("name")

                        values = variable.get("values") or []
                        for val_obj in values:
                            if not val_obj: continue

                            time_instant_str = val_obj.get("timeInstant")
                            if not time_instant_str: continue

                            # Corrección del formato de zona horaria (+01 a +01:00)
                            if time_instant_str.endswith("Z"):
                                time_instant_str = time_instant_str.replace("Z", "+00:00")
                            elif len(time_instant_str) >= 3 and time_instant_str[-3] in ["+", "-"]:
                                time_instant_str += ":00"

                            dt_obj = datetime.fromisoformat(time_instant_str)

                            p = Point("prediccion_meteo") \
                                .tag("ciudad", ciudad) \
                                .tag("lon", lon) \
                                .tag("lat", lat) \
                                .time(dt_obj)

                            if var_name == "wind":
                                if val_obj.get("moduleValue") is not None:
                                    p.field("wind_module", float(val_obj["moduleValue"]))
                                if val_obj.get("directionValue") is not None:
                                    p.field("wind_direction", float(val_obj["directionValue"]))
                            else:
                                valor = val_obj.get("value")
                                if valor is not None:
                                    try:
                                        p.field(var_name, float(valor))
                                    except ValueError:
                                        p.field(f"{var_name}_estado", str(valor))

                            puntos.append(p)

            # Si la API falló devolviendo nulls, evitamos intentar guardar una lista vacía
            if puntos:
                write_api.write(bucket=BUCKET, org=ORG, record=puntos)
                print(f" -> Guardados {len(puntos)} registros para {ciudad}")
            else:
                print(f" -> ⚠️ Advertencia: No se encontraron datos válidos para {ciudad} en este momento.")

            # Pausa de 1 segundo entre ciudades para no saturar la API
            time.sleep(1)

        print(f"[{datetime.now()}] Barrido de Galicia completado con éxito.")

    except Exception as e:
        print(f"[{datetime.now()}] Error general en el recolector: {e}")

# Ejecutar todos los días a las 12:40
schedule.every().day.at("12:40").do(obtener_y_guardar_datos)

print("Iniciando servicio recolector distribuido...")
obtener_y_guardar_datos() # Ejecución inicial al levantar el docker

while True:
    schedule.run_pending()
    time.sleep(60)
