import requests
import schedule
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- CONFIGURACIÓN AEMET ---
AEMET_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzLnZjYW1iYUB1ZGMuZXMiLCJqdGkiOiIxMWNlOTAzOS1jOWRiLTQ0NTktODBjYy0zODkwOGRlNTA4YWMiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc3MjI1MzY3MywidXNlcklkIjoiMTFjZTkwMzktYzlkYi00NDU5LTgwY2MtMzg5MDhkZTUwOGFjIiwicm9sZSI6IiJ9.GXfB_Zvj4FGwowmfxo73TKrftPj1y5LzYM1TB33r25E"
URL_AEMET = "https://opendata.aemet.es/opendata/api/observacion/convencional/todas"

# --- CONFIGURACIÓN INFLUXDB ---
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "l4dwrX-J3b7KCJVl9naBD2YTz9VsvE2zAAqML-hXJw9CSuNHu118qPQVTyEIzJwy32RcZPyaQFtuWM9pNb44vw=="
ORG = "galicia_data"
BUCKET = "meteo_bucket"

PROVINCIAS_GALICIA = ["A CORUÑA", "LUGO", "OURENSE", "PONTEVEDRA"]

def obtener_datos_aemet():
    print(f"[{datetime.now()}] Iniciando recolección AEMET...")
    headers = {"api_key": AEMET_API_KEY}
    
    try:
        # Paso 1: Obtener la URL temporal
        res_inicial = requests.get(URL_AEMET, headers=headers)
        res_inicial.raise_for_status()
        
        datos_url = res_inicial.json().get("datos")
        if not datos_url: return

        # Paso 2: Descargar los datos
        res_datos = requests.get(datos_url)
        res_datos.raise_for_status()
        observaciones = res_datos.json()
        
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        puntos = []

        for obs in observaciones:
            provincia = obs.get("provincia", "").upper()
            
            if provincia in PROVINCIAS_GALICIA:
                estacion = obs.get("ubi", "Desconocida")
                lat = obs.get("lat")
                lon = obs.get("lon")
                
                if lat and lon:
                    p = Point("aemet_actual") \
                        .tag("estacion", estacion) \
                        .tag("provincia", provincia) \
                        .field("lat", float(lat)) \
                        .field("lon", float(lon))
                        
                    if "ta" in obs: p.field("temperatura", float(obs["ta"]))
                    if "hr" in obs: p.field("humedad", float(obs["hr"]))
                    if "vv" in obs: p.field("viento_velocidad", float(obs["vv"]))
                    if "prec" in obs: p.field("precipitacion", float(obs["prec"]))
                    
                    p.time(datetime.utcnow())
                    puntos.append(p)

        if puntos:
            write_api.write(bucket=BUCKET, org=ORG, record=puntos)
            print(f"[{datetime.now()}] Guardadas {len(puntos)} estaciones AEMET Galicia.")
        
    except Exception as e:
        print(f"Error recolectando AEMET: {e}")

# Las observaciones de AEMET se actualizan cada hora
schedule.every().hour.at(":15").do(obtener_datos_aemet)
obtener_datos_aemet()

while True:
    schedule.run_pending()
    time.sleep(60)
