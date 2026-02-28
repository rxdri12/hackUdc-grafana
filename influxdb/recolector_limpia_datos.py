from datetime import datetime
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time 
import schedule

XUNTA_ICA_URL = "https://servizos.meteogalicia.gal/mgrss/caire/jsonICAActual.action"
#METEO_TOKEN = "O7d8a21OKeGZ20z9xN5YyMK4JM3U42sUq6MCpy8Jo90H9l3Y8We42Bpk8SMp5O9z"
METEO_TOKEN  =   "bZUUtsTTz6ShpFYbfu8q60YoLYQp29401sDwfszZAha84nWv6vfk5Q6FPojHRt9c"
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "l4dwrX-J3b7KCJVl9naBD2YTz9VsvE2zAAqML-hXJw9CSuNHu118qPQVTyEIzJwy32RcZPyaQFtuWM9pNb44vw=="
INFLUX_ORG = "galicia_data"
INFLUX_BUCKET = "mixed_bucket"
def extraer_serie_temporal(json_completo):
    """
    Pivota el JSON de MeteoGalicia. 
    En lugar de devolver el punto más cercano, devuelve la serie predictiva completa.
    """
    resumen_por_estacion = []
    
    for feature in json_completo.get('features', []):
        series_dict = {} # Diccionario temporal: { "2026-02-28T13:00:00": { "temperature": 15, ... } }
        
        for day in feature.get('properties', {}).get('days', []):
            for var in day.get('variables', []):
                nombre_var = var['name']
                
                for v in var.get('values', []):
                    t_str = v['timeInstant'][:19] 
                    
                    if t_str not in series_dict:
                        series_dict[t_str] = {}
                    
                    # El viento usa moduleValue, el resto usa value
                    if nombre_var == 'wind':
                        series_dict[t_str]['viento_velocidad'] = v.get('moduleValue', 0)
                    else:
                        series_dict[t_str][nombre_var] = v.get('value')
        
        # Convertimos el diccionario temporal a una lista de predicciones para esta estación
        lista_predicciones = []
        for t_str, vars_clima in series_dict.items():
            vars_clima['timeInstant'] = t_str
            lista_predicciones.append(vars_clima)
            
        resumen_por_estacion.append(lista_predicciones)
        
    return resumen_por_estacion

def obtener_datos_meteo(estaciones_ica, use_mock=False):
    datos_fusionados_totales = []
    
    if use_mock:
        print("Usando datos locales: mock_meteo_raw.json")
        try:
            with open('mock_meteo_raw.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            series_temporales = extraer_serie_temporal(data)
            
            # Asumiendo que el mock tiene el mismo número y orden de estaciones que estaciones_ica
            for estacion, lista_predicciones in zip(estaciones_ica, series_temporales):
                for prediccion in lista_predicciones:
                    estacion_fusionada = {**estacion, **prediccion}
                    datos_fusionados_totales.append(estacion_fusionada)
            return datos_fusionados_totales
        except FileNotFoundError:
            print("Error: No se encontró mock_meteo_raw.json")
            return []

    # Lógica original para la API si no se usa mock
    base_url = "https://servizos.meteogalicia.gal/apiv5/getNumericForecastInfo"
    tamaño_lote = 20
    chunks = [estaciones_ica[i:i + tamaño_lote] for i in range(0, len(estaciones_ica), tamaño_lote)]

    for indice, chunk in enumerate(chunks):
        coords_list = [f"{item['longitud']},{item['latitud']}" for item in chunk]
        coords_param = ";".join(coords_list)
        url_final = f"{base_url}?coords={coords_param}&API_KEY={METEO_TOKEN}"

        try:
            responser = requests.get(url_final)
            responser.raise_for_status()
            data = responser.json()

            series_temporales = extraer_serie_temporal(data)

            # CRUCE: 1 estación ICA x N predicciones horarias
            for estacion, lista_predicciones in zip(chunk, series_temporales):
                for prediccion in lista_predicciones:
                    estacion_fusionada = {**estacion, **prediccion}
                    datos_fusionados_totales.append(estacion_fusionada)
                
        except requests.exceptions.RequestException as e:
            print(f"Error procesando el lote {indice + 1}: {e}")

    return datos_fusionados_totales

def guardar_en_influxdb(lista_datos):
    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        puntos_a_insertar = []
        
        for dato in lista_datos:
            # Evaluamos el deporte para CADA punto horario predictivo
            dato = evaluar_dia_deporte(dato)
            try:
                # Parseamos el tiempo de la predicción, no el actual
                tiempo_prediccion = datetime.strptime(dato['timeInstant'], "%Y-%m-%dT%H:%M:%S")
                
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
                    .field("nota_deporte", float(dato.get("nota_deporte", 0))) \
                    .time(tiempo_prediccion, WritePrecision.S) \ # <- CRÍTICO: Inserta el dato en su tiempo futuro
                    .field("rating", float(dato["nota_deporte"]))
                
                puntos_a_insertar.append(punto)
            
            except (ValueError, KeyError) as e:
                print(f"Error procesando los datos de la estación {dato.get('estacion')} en tiempo {dato.get('timeInstant')}: {e}")
                continue

        # Inserción en batch
        if puntos_a_insertar:
            # Dividir en lotes si hay muchísimos puntos (opcional pero recomendado en series largas)
            batch_size = 1000
            for i in range(0, len(puntos_a_insertar), batch_size):
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=puntos_a_insertar[i:i+batch_size])
            print(f"Se han guardado {len(puntos_a_insertar)} predicciones en InfluxDB.")
        else:
            print("No hay datos válidos para insertar.")

# Ejecución
primeros = obtener_datos_ica()
if primeros:
    # Cambia a True para probar con el mock
    resultado_final = obtener_datos_meteo(primeros, use_mock=True) 
    guardar_en_influxdb(resultado_final)
