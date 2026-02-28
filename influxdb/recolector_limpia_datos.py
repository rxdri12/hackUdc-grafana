from datetime import datetime
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time 
import schedule

XUNTA_ICA_URL = "https://servizos.meteogalicia.gal/mgrss/caire/jsonICAActual.action"
METEO_TOKEN = "O7d8a21OKeGZ20z9xN5YyMK4JM3U42sUq6MCpy8Jo90H9l3Y8We42Bpk8SMp5O9z"

INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "l4dwrX-J3b7KCJVl9naBD2YTz9VsvE2zAAqML-hXJw9CSuNHu118qPQVTyEIzJwy32RcZPyaQFtuWM9pNb44vw=="
INFLUX_ORG = "galicia_data"
INFLUX_BUCKET = "mixed_bucket"

def eliminar_campos(data):
    campos_a_borrar = ['fecha', 'maximo', 'color', 'idEstacion','idParametro', 'icaEn']
    estaciones = data.get('icas', [])
    
    for estacion in estaciones:
        for campo in campos_a_borrar:
            estacion.pop(campo, None) 
    return estaciones

def obtener_datos_ica():
    print("Consultando MeteoSIX v5...")
    try:
        responser = requests.get(XUNTA_ICA_URL)
        responser.raise_for_status()
        data = responser.json()

        data = eliminar_campos(data)

        return data
    
    except Exception as e:
        print(f" Error: {e}")


def simplificar_meteo(json_completo):
    ahora_dt = datetime.now()
    resumen = []
    
    for feature in json_completo.get('features', []):
        # Ya no necesitamos guardar las coords aquí porque las tenemos en la lista ICA original
        datos_climaticos = {}
        min_diff_global = {}
        
        for day in feature.get('properties', {}).get('days', []):
            for var in day.get('variables', []):
                nombre_var = var['name']
                
                if nombre_var not in min_diff_global:
                    min_diff_global[nombre_var] = float('inf')
                
                for v in var.get('values', []):
                    tiempo_api_str = v['timeInstant'][:19] 
                    tiempo_api_dt = datetime.strptime(tiempo_api_str, "%Y-%m-%dT%H:%M:%S")
                    
                    diff = abs((ahora_dt - tiempo_api_dt).total_seconds())
                    
                    if diff < min_diff_global[nombre_var]:
                        min_diff_global[nombre_var] = diff
                        
                        if nombre_var == 'wind':
                            datos_climaticos['viento_velocidad'] = v['moduleValue']
                        else:
                            datos_climaticos[nombre_var] = v['value']
        
        resumen.append(datos_climaticos)
        
    return resumen

def evaluar_dia_deporte(estacion_meteo):
    # Extraemos las métricas del diccionario
    temp = estacion_meteo.get('temperature', 16)
    viento = estacion_meteo.get('viento_velocidad', 0)
    lluvia = estacion_meteo.get('precipitation_amount', 0)
    cielo = estacion_meteo.get('sky_state', 'SUNNY')
    ica_valor = estacion_meteo.get('valor', 0)

    nota = 100.0

    # 1. Factor de Estado del Cielo (Penalización base cualitativa)
    # Castigamos estados molestos que no necesariamente implican mucha lluvia acumulada (ej. niebla, llovizna)
    penalizaciones_cielo = {
        'SUNNY': 0, 'PARTLY_CLOUDY': 0, 'CLOUDY': 0, 'OVERCAST': 0,
        'HIGH_CLOUDS': 0, 'MID_CLOUDS': 0,
        'FOG': 15, 'MIST': 10, 'FOG_BANK': 10,
        'WEAK_SHOWERS': 15, 'OVERCAST_AND_SHOWERS': 25, 'SHOWERS': 30,
        'DRIZZLE': 20, 'RAIN': 40, 'STORMS': 100
    }
    nota -= penalizaciones_cielo.get(cielo, 0)

    # 2. Factor de Lluvia Cuantitativa (Penaliza severamente los litros/m2)
    # 1 l/m2 en una hora ya te empapa si estás corriendo o en bici.
    if lluvia > 0:
        nota -= (lluvia * 15)

    # 3. Factor de Temperatura (Campana: el óptimo está en ~16ºC)
    # Elevar al cuadrado hace que 20ºC reste poco, pero 2ºC o 35ºC hundan la nota.
    desviacion_temp = abs(temp - 16.0)
    nota -= (desviacion_temp ** 2) * 0.15

    # 4. Factor de Viento
    # Hasta 15 km/h es brisa. Por encima de eso, aumenta la resistencia y la sensación térmica.
    if viento > 15:
        nota -= (viento - 15) * 1.5

    # 5. Factor de Calidad del Aire (ICA)
    # Usamos la métrica 'valor' (escala 0-500).
    if ica_valor <= 50:
        pass # Aire limpio
    elif ica_valor <= 100:
        # Penalización lineal suave para calidades regulares
        nota -= (ica_valor - 50) * 0.3
    else:
        # VETO: Si supera 100, hiperventilar es dañino. Se aplica factor multiplicativo destructivo.
        multiplicador = max(0.1, 1.0 - ((ica_valor - 100) / 100))
        nota *= multiplicador

    # Asegurar límites estrictos
    nota_final = max(0.0, min(100.0, nota))

    # Inyectamos la nota calculada en el propio objeto
    estacion_meteo['nota_deporte'] = round(nota_final, 1)

    return estacion_meteo

def obtener_datos_meteo(estaciones_ica):
    base_url = "https://servizos.meteogalicia.gal/apiv5/getNumericForecastInfo"
    datos_fusionados_totales = []
    
    # Segmentamos la lista en bloques de 20 (límite de la API)
    tamaño_lote = 20
    chunks = [estaciones_ica[i:i + tamaño_lote] for i in range(0, len(estaciones_ica), tamaño_lote)]

    for indice, chunk in enumerate(chunks):
        # Opcional: print para monitorizar el progreso en logs
        print(f"Procesando lote {indice + 1}/{len(chunks)} con {len(chunk)} estaciones...")
        
        coords_list = [f"{item['longitud']},{item['latitud']}" for item in chunk]
        coords_param = ";".join(coords_list)

        url_final = f"{base_url}?coords={coords_param}&API_KEY={METEO_TOKEN}"

        try:
            responser = requests.get(url_final)
            responser.raise_for_status()
            data = responser.json()

            # Reutilizamos tu función de simplificación (asegúrate de que está corregida)
            datos_climaticos_simplificados = simplificar_meteo(data)

            # CRUCE DE DATOS del lote actual
            for estacion, clima in zip(chunk, datos_climaticos_simplificados):
                estacion_fusionada = {**estacion, **clima}
                datos_fusionados_totales.append(estacion_fusionada)
                
        except requests.exceptions.RequestException as e:
            # Es crítico capturar el error por lote, no abortar todo el proceso
            # Si el lote 2 falla por timeout, el lote 3 debería intentar ejecutarse
            print(f"Error procesando el lote {indice + 1}: {e}")

    return datos_fusionados_totales

def guardar_en_influxdb(lista_datos):
    # Usamos un context manager para asegurar que las conexiones se cierran
    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        # Iniciamos la API de escritura
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        puntos_a_insertar = []
        
        for dato in lista_datos:
            dato = evaluar_dia_deporte(dato)
            try:
                # Creación del punto de datos (measurement: "estado_aire_meteo")
                punto = Point("estado_aire_meteo") \
                    .tag("estacion", dato.get("estacion", "Desconocida")) \
                    .tag("estado_cielo", dato.get("sky_state", "Desconocido")) \
                    .tag("calidad_es", dato.get("icaEs", "Desconocida")) \
                    .field("ica", float(dato.get("ica", 0))) \
                    .field("valor_ica", float(dato.get("valor", 0))) \
                    .field("temperatura", float(dato.get("temperature", 0))) \
                    .field("precipitacion", float(dato.get("precipitation_amount", 0))) \
                    .field("viento_velocidad", float(dato.get("viento_velocidad", 0))) \
                    .field("latitud", float(dato["latitud"])) \
                    .field("longitud", float(dato["longitud"])) \
                    .field("rating", float(dato["rating"]))
                
                puntos_a_insertar.append(punto)
            
            except (ValueError, KeyError) as e:
                print(f"Error procesando los datos de la estación {dato.get('estacion')}: {e}")
                # Decisión de diseño: si un punto falla, lo ignoramos y seguimos con el resto
                continue

        # Escritura en bloque (batch) a la base de datos
        if puntos_a_insertar:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=puntos_a_insertar)
            print(f"Se han guardado {len(puntos_a_insertar)} registros en InfluxDB.")
        else:
            print("No hay datos válidos para insertar.")

primeros = obtener_datos_ica()
# Ejecución principal
if primeros:
    resultado_final = obtener_datos_meteo(primeros)
    guardar_en_influxdb(resultado_final)


def tarea_diaria():
    print(f"[{datetime.now()}] Iniciando recolección de datos...")
    primeros = obtener_datos_ica()
    if primeros:
        resultado_final = obtener_datos_meteo(primeros)
        guardar_en_influxdb(resultado_final)
    print("Tarea finalizada.")

# Programar la tarea todos los días a las 12:40
schedule.every().day.at("12:40").do(tarea_diaria)

if __name__ == "__main__":
    print("Script iniciado. Esperando para ejecutarse a las 12:40...")
    while True:
        schedule.run_pending()
        time.sleep(60) # Pausa de 60 segundos para no saturar la CPU

