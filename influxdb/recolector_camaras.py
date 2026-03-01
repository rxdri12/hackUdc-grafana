import requests
import schedule
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os 

# --- CONFIGURACIÓN ---
URL_CAMARAS = "https://servizos.meteogalicia.gal/mgrss/observacion/jsonCamaras.action"
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
ORG = "galicia_data"
BUCKET = "mixed_bucket"

def obtener_camaras():
    print(f"[{datetime.now()}] Descargando red de webcams...")
    try:
        res = requests.get(URL_CAMARAS)
        res.raise_for_status()
        data = res.json()
        
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        puntos = []

        for cam in data.get("listaCamaras", []):
            concello = cam.get("concello", "Desconocido").upper()
            nombre_cam = cam.get("nomeCamara", "")
            lat = cam.get("lat")
            lon = cam.get("lon")
            url_img = cam.get("imaxeCamara", "")
            
            # Limpiamos las barras escapadas (\/) del JSON a formato web normal (/)
            url_img = url_img.replace("\\/", "/")

            if lat and lon and url_img:
                # Guardamos lat, lon y la URL en un measurement nuevo
                p = Point("camaras_directo") \
                    .tag("concello", concello) \
                    .tag("nombre_camara", nombre_cam) \
                    .field("latitud", float(lat)) \
                    .field("longitud", float(lon)) \
                    .field("url_foto", url_img) \
                    .time(datetime.utcnow())
                puntos.append(p)

        if puntos:
            write_api.write(bucket=BUCKET, org=ORG, record=puntos)
            print(f"[{datetime.now()}] ✅ Guardadas {len(puntos)} cámaras en InfluxDB.")
            
    except Exception as e:
        print(f"Error recolectando cámaras: {e}")

# Las cámaras se actualizan cada ~10 minutos, así que corremos el script cada 10 mins
schedule.every(10).minutes.do(obtener_camaras)

obtener_camaras() # Ejecución inicial
while True:
    schedule.run_pending()
    time.sleep(60)
