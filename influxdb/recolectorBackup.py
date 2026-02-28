import requests
import schedule
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- CONFIGURACIÓN ---
METEO_TOKEN = "O7d8a21OKeGZ20z9xN5YyMK4JM3U42sUq6MCpy8Jo90H9l3Y8We42Bpk8SMp5O9z"
# Coordenadas de ejemplo (A Coruña): Longitud, Latitud
LON, LAT = "-8.41", "43.36" 
# Variables a extraer (separadas por comas)
VARIABLES = "temperature,wind,precipitation_amount,relative_humidity,sky_state,significative_wave_height"

# URL base de la API v5
API_URL = f"https://servizos.meteogalicia.gal/apiv5/getNumericForecastInfo?coords={LON},{LAT}&variables={VARIABLES}&API_KEY={METEO_TOKEN}"

INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "l4dwrX-J3b7KCJVl9naBD2YTz9VsvE2zAAqML-hXJw9CSuNHu118qPQVTyEIzJwy32RcZPyaQFtuWM9pNb44vw==" # Generado dentro de InfluxDB, el token de influxdb
ORG = "galicia_data"
BUCKET = "meteo_bucket"

def obtener_y_guardar_datos():
    print(f"[{datetime.now()}] Consultando MeteoSIX v5...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        puntos = []

        # Parsear el GeoJSON de la v5
        for feature in data.get("features", []):
            coords = feature.get("geometry", {}).get("coordinates", [0, 0])
            propiedades = feature.get("properties", {})
            
            for day in propiedades.get("days", []):
                for variable in day.get("variables", []):
                    var_name = variable.get("name")
                    
                    for val_obj in variable.get("values", []):
                        if not val_obj: continue
                        
                        time_instant_str = val_obj.get("timeInstant")
                        if not time_instant_str: continue
                        
                        # Convertir a datetime asumiendo formato yyyy-MM-ddTHH:mm:ss+XX
                       # Arreglar el formato del huso horario de MeteoGalicia (+01 a +01:00)
                        if time_instant_str.endswith("Z"):
                            time_instant_str = time_instant_str.replace("Z", "+00:00")
                        elif len(time_instant_str) >= 3 and time_instant_str[-3] in ["+", "-"]:
                            time_instant_str += ":00"

                        # Ahora sí, convertir a datetime
                        dt_obj = datetime.fromisoformat(time_instant_str) 
                        # Crear el punto base
                        p = Point("prediccion_meteo") \
                            .tag("lon", str(coords[0])) \
                            .tag("lat", str(coords[1])) \
                            .time(dt_obj)

                        # El viento es especial: devuelve moduleValue y directionValue
                        if var_name == "wind":
                            if val_obj.get("moduleValue") is not None:
                                p.field("wind_module", float(val_obj["moduleValue"]))
                            if val_obj.get("directionValue") is not None:
                                p.field("wind_direction", float(val_obj["directionValue"]))
                        else:
                            valor = val_obj.get("value")
                            if valor is not None:
                                # Algunas variables (como sky_state) devuelven texto
                                try:
                                    p.field(var_name, float(valor))
                                except ValueError:
                                    p.field(f"{var_name}_estado", str(valor))
                                    
                        puntos.append(p)

        write_api.write(bucket=BUCKET, org=ORG, record=puntos)
        print(f"[{datetime.now()}] Datos guardados: {len(puntos)} registros introducidos.")

    except Exception as e:
        print(f"[{datetime.now()}] Error: {e}")

# Ejecutar todos los días a las 12:40 (MeteoGalicia actualiza a las 12:30 aprox)
schedule.every().day.at("12:40").do(obtener_y_guardar_datos)

print("Iniciando servicio recolector...")
obtener_y_guardar_datos() # Ejecución inicial

while True:
    schedule.run_pending()
    time.sleep(60)
