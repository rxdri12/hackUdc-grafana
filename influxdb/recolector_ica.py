import requests
import schedule
import time
import csv
import os
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- CONFIGURACIÓN ---
URL_ICA_ACTUAL = "https://servizos.meteogalicia.gal/mgrss/caire/jsonICAActual.action"
URL_ICA_PRED = "https://servizos.meteogalicia.gal/mgrss/caire/jsonPrediccionIcaDiarioConcello.action"
CSV_FILE = "concellos.csv"

INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "l4dwrX-J3b7KCJVl9naBD2YTz9VsvE2zAAqML-hXJw9CSuNHu118qPQVTyEIzJwy32RcZPyaQFtuWM9pNb44vw=="
ORG = "galicia_data"
BUCKET = "meteo_bucket"

def normalizar_nombre(nombre):
    """Limpia el nombre y arregla casos como 'Porriño, O' -> 'O PORRIÑO'"""
    nombre = nombre.strip().upper()
    if ", O" in nombre:
        nombre = "O " + nombre.replace(", O", "")
    elif ", A" in nombre:
        nombre = "A " + nombre.replace(", A", "")
    elif ", OS" in nombre:
        nombre = "OS " + nombre.replace(", OS", "")
    elif ", AS" in nombre:
        nombre = "AS " + nombre.replace(", AS", "")
    return nombre

def cargar_coordenadas_csv():
    """Carga el CSV en un diccionario: {'NOMBRE_NORMALIZADO': (lat, lon)}"""
    coords_dict = {}
    if not os.path.exists(CSV_FILE):
        print(f"⚠️ ERROR: No se encuentra el archivo {CSV_FILE}")
        return coords_dict

    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Usamos los nombres exactos de las columnas de tu CSV
            nombre = normalizar_nombre(row['Nombre_Concello'])
            try:
                # Ojo al orden: Guardamos Latitud y Longitud como floats
                coords_dict[nombre] = (float(row['Latitud']), float(row['Longitud']))
            except (ValueError, TypeError, KeyError) as e:
                # Si alguna fila está en blanco o mal formada, la salta
                continue
                
    print(f"✅ CSV cargado: {len(coords_dict)} concellos mapeados y listos.")
    return coords_dict

def obtener_datos_ica():
    print(f"[{datetime.now()}] Iniciando recolección de Calidad del Aire (ICA)...")
    coords_referencia = cargar_coordenadas_csv()
    
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        puntos = []

        # 1. OBTENER ICA ACTUAL (Estos ya traen lat y lon de la Xunta)
        print(" -> Descargando ICA Actual...")
        resp_actual = requests.get(URL_ICA_ACTUAL)
        resp_actual.raise_for_status()
        data_actual = resp_actual.json()

        for estacion in data_actual.get("icas", []):
            nombre = estacion.get("estacion", "Desconocida")
            lat = estacion.get("latitud")
            lon = estacion.get("longitud")
            ica_valor = estacion.get("ica")

            # Filtramos valores negativos (estaciones caídas "Sen Datos")
            if lat and lon and ica_valor is not None and float(ica_valor) >= 0:
                p = Point("ica_actual") \
                    .tag("estacion", nombre) \
                    .field("ica", float(ica_valor)) \
                    .field("lat", float(lat)) \
                    .field("lon", float(lon)) \
                    .time(datetime.utcnow())
                puntos.append(p)

        # 2. OBTENER PREDICCIÓN ICA (Aquí usamos nuestro diccionario CSV)
        print(" -> Descargando Predicción ICA...")
        resp_pred = requests.get(URL_ICA_PRED)
        resp_pred.raise_for_status()
        data_pred = resp_pred.json()

        for prediccion in data_pred.get("prediccion", []):
            # Normalizamos el nombre que nos da la Xunta para que sea idéntico al CSV
            nombre_json = normalizar_nombre(prediccion.get("concello", ""))
            ica_pred = prediccion.get("ica")
            
            # Buscamos en nuestro diccionario
            coords = coords_referencia.get(nombre_json)

            if coords and ica_pred is not None:
                lat_csv, lon_csv = coords
                p = Point("ica_prediccion") \
                    .tag("concello", nombre_json) \
                    .field("ica", float(ica_pred)) \
                    .field("lat", lat_csv) \
                    .field("lon", lon_csv) \
                    .time(datetime.utcnow())
                puntos.append(p)

        # 3. ESCRIBIR EN INFLUXDB
        if puntos:
            write_api.write(bucket=BUCKET, org=ORG, record=puntos)
            print(f"[{datetime.now()}] Éxito: {len(puntos)} registros de calidad de aire guardados en InfluxDB.")
        else:
            print(f"[{datetime.now()}] Aviso: No se generaron puntos válidos.")

    except Exception as e:
        print(f"[{datetime.now()}] Error en el recolector ICA: {e}")

# Programación: cada hora exacta (ej. 13:00, 14:00)
schedule.every().hour.at(":00").do(obtener_datos_ica)

print("Iniciando servicio recolector de Calidad del Aire (ICA)...")
obtener_datos_ica() # Ejecuta la primera vez al arrancar el contenedor

while True:
    schedule.run_pending()
    time.sleep(60)
