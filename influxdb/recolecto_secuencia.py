from datetime import datetime
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import schedule
import json
import os 

XUNTA_ICA_URL = "https://servizos.meteogalicia.gal/mgrss/caire/jsonICAActual.action"
METEO_TOKEN = os.getenv("METEO_TOKEN")

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = "galicia_data"
INFLUX_BUCKET = "mixed_bucket"

def eliminar_campos(data):
    campos_a_borrar = ['fecha', 'color', 'idEstacion','idParametro', 'icaEn']
    estaciones = data.get('icas', [])

    for estacion in estaciones:
        for campo in campos_a_borrar:
            estacion.pop(campo, None)
    return estaciones

def obtener_datos_ica():
    print("Consultando MeteoSIX v5 (ICA)...")
    try:
        responser = requests.get(XUNTA_ICA_URL)
        responser.raise_for_status()
        data = responser.json()
        data = eliminar_campos(data)
        return data
    except Exception as e:
        print(f" Error obteniendo ICA: {e}")
        return []

def simplificar_meteo(json_completo):
    """
    Ahora devuelve una LISTA DE LISTAS. 
    Para cada estación, devuelve una lista con todos sus puntos horarios de previsión.
    """
    resumen_estaciones = []

    for feature in json_completo.get('features', []):
        previsiones_por_hora = {}

        for day in feature.get('properties', {}).get('days', []):
            for var in day.get('variables', []):
                nombre_var = var['name']

                for v in var.get('values', []):
                    # Ignorar tramos sin datos (nulos)
                    if v.get('value') is None and v.get('moduleValue') is None:
                        continue

                    # La API devuelve formato yyyy-MM-ddTHH:mm:ssZZ
                    tiempo_api_str = v['timeInstant'][:19]
                    
                    if tiempo_api_str not in previsiones_por_hora:
                        previsiones_por_hora[tiempo_api_str] = {'timestamp': tiempo_api_str}

                    if nombre_var == 'wind':
                        previsiones_por_hora[tiempo_api_str]['viento_velocidad'] = v['moduleValue']
                    else:
                        previsiones_por_hora[tiempo_api_str][nombre_var] = v['value']

        resumen_estaciones.append(list(previsiones_por_hora.values()))

    return resumen_estaciones

def evaluar_dia_deporte(estacion_meteo):
    temp = estacion_meteo.get('temperature', 16)
    viento = estacion_meteo.get('viento_velocidad', 0)
    lluvia = estacion_meteo.get('precipitation_amount', 0)
    cielo = estacion_meteo.get('sky_state', 'SUNNY')
    ica_valor = estacion_meteo.get('valor', 0)

    nota = 100.0

    penalizaciones_cielo = {
        'SUNNY': 0, 'PARTLY_CLOUDY': 0, 'CLOUDY': 0, 'OVERCAST': 0,
        'HIGH_CLOUDS': 0, 'MID_CLOUDS': 0,
        'FOG': 15, 'MIST': 10, 'FOG_BANK': 10,
        'WEAK_SHOWERS': 15, 'OVERCAST_AND_SHOWERS': 25, 'SHOWERS': 30,
        'DRIZZLE': 20, 'RAIN': 40, 'STORMS': 100
    }
    nota -= penalizaciones_cielo.get(cielo, 0)

    if lluvia > 0:
        nota -= (lluvia * 15)

    desviacion_temp = abs(temp - 16.0)
    nota -= (desviacion_temp ** 2) * 0.15

    if viento > 15:
        nota -= (viento - 15) * 1.5

    if ica_valor <= 50:
        pass 
    elif ica_valor <= 100:
        nota -= (ica_valor - 50) * 0.3
    else:
        multiplicador = max(0.1, 1.0 - ((ica_valor - 100) / 100))
        nota *= multiplicador

    nota_final = max(0.0, min(100.0, nota))
    estacion_meteo['nota_deporte'] = round(nota_final, 1)

    return estacion_meteo

def obtener_datos_meteo(estaciones_ica, use_mock=False):
    datos_fusionados_totales = []

    if use_mock:
        print("Cargando clima desde mock_meteo_raw.json...")
        try:
            with open('mock_meteo_raw.json', 'r', encoding='utf-8') as archivo:
                data = json.load(archivo)
            
            datos_climaticos_simplificados = simplificar_meteo(data)
            
            # Cruzamos 1 estación con N previsiones horarias
            for estacion, previsiones in zip(estaciones_ica, datos_climaticos_simplificados):
                for prev in previsiones:
                    estacion_fusionada = {**estacion, **prev}
                    datos_fusionados_totales.append(estacion_fusionada)
                
            return datos_fusionados_totales
        except FileNotFoundError:
            print("Error: mock_meteo_raw.json no encontrado.")
            return []

    # OJO: Recuerda añadir &autoAdjustPosition=false si necesitas precisión exacta de coordenadas
    base_url = "https://servizos.meteogalicia.gal/apiv5/getNumericForecastInfo"
    tamaño_lote = 20
    chunks = [estaciones_ica[i:i + tamaño_lote] for i in range(0, len(estaciones_ica), tamaño_lote)]

    for indice, chunk in enumerate(chunks):
        print(f"Descargando clima API: Lote {indice + 1}/{len(chunks)}...")

        coords_list = [f"{item['longitud']},{item['latitud']}" for item in chunk]
        coords_param = ";".join(coords_list)

        url_final = f"{base_url}?coords={coords_param}&API_KEY={METEO_TOKEN}&autoAdjustPosition=false"

        try:
            responser = requests.get(url_final)
            responser.raise_for_status()
            data = responser.json()

            datos_climaticos_simplificados = simplificar_meteo(data)

            for estacion, previsiones in zip(chunk, datos_climaticos_simplificados):
                for prev in previsiones:
                    estacion_fusionada = {**estacion, **prev}
                    datos_fusionados_totales.append(estacion_fusionada)

        except requests.exceptions.RequestException as e:
            print(f"Error procesando el lote {indice + 1}: {e}")

    return datos_fusionados_totales

def guardar_en_influxdb(lista_datos):
    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        puntos_a_insertar = []

        for dato in lista_datos:
            dato = evaluar_dia_deporte(dato)
            try:
                # Parseamos el timestamp ISO 8601 con offset de MeteoGalicia
                dt_obj = datetime.strptime(dato["timestamp"], "%Y-%m-%dT%H:%M:%S")

                # Insertamos usando el tiempo de la predicción (.time), no el actual
                punto = Point("estado_aire_meteo") \
                    .time(dt_obj, WritePrecision.S) \
                    .tag("estacion", dato["estacion"]) \
                    .tag("estado_cielo", dato["sky_state"]) \
                    .tag("calidad_es", dato["icaEs"]) \
                    .field("ica", float(dato["ica"])) \
                    .field("valor_ica", float(dato["valor"])) \
                    .field("temperatura", float(dato["temperature"])) \
                    .field("precipitacion", float(dato["precipitation_amount"])) \
                    .field("viento_velocidad", float(dato["viento_velocidad"])) \
                    .field("latitud", float(dato["latitud"])) \
                    .field("longitud", float(dato["longitud"])) \
                    .field("rating", float(dato["nota_deporte"])) 

                puntos_a_insertar.append(punto)

            except KeyError as e:
                print(f"Error: Faltan datos clave para insertar. Falta: {e} en la estación {dato.get('estacion', 'Desconocida')} a las {dato.get('timestamp')}")
                continue
            except ValueError as e:
                print(f"Error parseando fecha {dato.get('timestamp')}: {e}")
                continue

        if puntos_a_insertar:
            # Batching nativo de InfluxDB para no saturar si hay miles de puntos
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=puntos_a_insertar)
            print(f"¡ÉXITO! Se han guardado {len(puntos_a_insertar)} predicciones horarias en InfluxDB.")
        else:
            print("No se ha guardado ningún dato.")

def tarea_diaria():
    print(f"[{datetime.now()}] Iniciando recolección programada...")
    estaciones = obtener_datos_ica()
    if estaciones:
        # En la tarea programada forzamos a usar la API de MeteoGalicia (use_mock=False)
        resultado_final = obtener_datos_meteo(estaciones, use_mock=False)
        guardar_en_influxdb(resultado_final)
    print("Tarea finalizada.")


# ==========================================
# FLUJO DE EJECUCIÓN PRINCIPAL
# ==========================================

if __name__ == "__main__":
    print("Iniciando script...")
    
    # 1. Obtenemos la base (Estaciones, Coordenadas e ICA)
    primeros = obtener_datos_ica()
    
    if primeros:
        # 2. Cruzamos la base con el Clima. 
        # Pon use_mock=False si quieres tirar de la API real ahora mismo en vez del archivo local.
        resultado_final = obtener_datos_meteo(primeros, use_mock=True)
        
        # 3. Guardamos TODO junto en InfluxDB
        guardar_en_influxdb(resultado_final)
    else:
        print("No se pudo obtener la base de estaciones de la Xunta.")

    # --- Programador (Descomentar para dejar corriendo en Docker) ---
    # schedule.every().day.at("12:40").do(tarea_diaria)
    # print("Esperando a la próxima ejecución programada...")
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)
