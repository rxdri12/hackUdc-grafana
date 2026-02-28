import logging
import sqlite3
import os
import csv
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from influxdb_client import InfluxDBClient
import requests

# --- CONFIGURACIÓN ---
TOKEN = os.getenv("TELEGRAM_TOKEN", "8736884501:AAH2e1BuT3FkKDTami4Cpjs7PGe-0HNl0_w")
DB_PATH = "/app/data/usuarios.db"
CSV_PATH = "concellos.csv"

# Ojo: Usamos /api/chat porque estás enviando el formato "messages"
OLLAMA_URL = os.getenv("OLLAMA_HOST", "http://ollama:11434") + "/api/chat" 
MODELO_IA = "phi3" # Asegúrate de haberlo descargado en el contenedor

# Configuración InfluxDB
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = "galicia_data" # IMPORTANTE: En tus scripts anteriores era galicia_data, revísalo
INFLUX_BUCKET = "meteo_bucket"

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- BASE DE DATOS ---
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS usuarios (chat_id INTEGER PRIMARY KEY)''')
    c.execute('''CREATE TABLE IF NOT EXISTS suscripciones
              (chat_id INTEGER, concello TEXT, PRIMARY KEY (chat_id, concello))''')
    conn.commit()
    conn.close()
    print("✅ Base de datos SQLite inicializada.")

# --- IA ---
def obtener_consejo_ia(concello, temp, hum, velocidadViento, ica):
    """Envía los datos a Ollama para obtener una recomendación humana."""
    prompt = (
        f"Eres un asistente meteorológico gallego experto. "
        f"Datos actuales en {concello}: Temperatura {temp}ºC, Humedad {hum}%, Velocidad del viento {velocidadViento} km/h, Calidad del aire: {ica}. "
        f"Escribe un mensaje muy breve (máximo 4 frases y mínimo 2) en español dando un consejo práctico "
        f"sobre qué ropa llevar, si hace falta paraguas, si llueve demasiado fuerte, si va a hacer mucho viento, si es recomendable no salir por precaución, etc. Sé amable y cercano."
    )
    
    payload = {
        "model": MODELO_IA,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False # Muy importante para que devuelva todo de golpe
    }
    
    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=240)
        if response.status_code == 200:
            data = response.json()
            # Así es como Ollama devuelve la respuesta en /api/chat
            return data.get("message", {}).get("content", "No tengo consejo para ti.")
        else:
            logging.error(f"Error IA: {response.status_code} - {response.text}")
            return "La IA está descansando ahora mismo."
    except Exception as e:
        logging.error(f"Error conexión IA: {e}")
        return "No puedo obtener un consejo en este momento."

# --- INFLUXDB ---
# --- INFLUXDB ---
# --- INFLUXDB ---
def get_meteo_actual(concello):
    """Consulta InfluxDB (Meteo e ICA por separado)"""
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    datos_completos = {}
    
    logging.info(f"🔍 [METEOGALICIA] Iniciando búsqueda para el concello: {concello}")
    
    # 1. METEO GALICIA
    query_meteo = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -12h, stop: 24h)
      |> filter(fn: (r) => r["_measurement"] == "prediccion_meteo")
      |> filter(fn: (r) => r["ciudad"] =~ /(?i){concello}/)
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    logging.info(f"📄 Query MeteoGalicia a ejecutar:\n{query_meteo}")
    try:
        res_meteo = query_api.query(query_meteo)
        logging.info(f"✅ Respuesta Query MeteoGalicia: {len(res_meteo)} tablas encontradas.")
        
        if res_meteo and len(res_meteo) > 0:
            rec = res_meteo[0].records[0] # <--- ¡PRIMERO ASIGNAMOS LA VARIABLE!
            logging.info(f"📊 Datos crudos extraídos de MeteoGalicia: {rec.values}") # <--- ¡LUEGO LA IMPRIMIMOS!
            datos_completos['temperature'] = rec.values.get('temperature', 'N/A')
            datos_completos['relative_humidity'] = rec.values.get('relative_humidity', 'N/A')
            datos_completos['wind_module'] = rec.values.get('wind_module', 'N/A')
        else:
            logging.warning(f"⚠️ MeteoGalicia devolvió vacío para {concello}.")
    except Exception as e:
        logging.error(f"Error Influx Meteo: {e}")

    # 2. ICA PREDICCIÓN
    query_ica = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -24h, stop: 24h)
      |> filter(fn: (r) => r["_measurement"] == "ica_prediccion")
      |> filter(fn: (r) => r["concello"] =~ /(?i){concello}/)
      |> last()
    '''

    logging.info(f"📄 Query ICA a ejecutar:\n{query_ica}")

    try:
        res_ica = query_api.query(query_ica)
        logging.info(f"✅ Respuesta Query ICA: {len(res_ica)} tablas encontradas.")
        if res_ica and len(res_ica) > 0:
            datos_completos['ica'] = res_ica[0].records[0].get_value()
            logging.info(f"🍃 ICA extraído: {datos_completos['ica']}")
        else:
            logging.warning(f"⚠️ ICA devolvió vacío para {concello}.")
    except Exception as e:
        logging.error(f"Error Influx ICA: {e}")

    client.close()

    if datos_completos:
        datos_completos['fuente'] = 'MeteoGalicia'
        logging.info(f"🚀 Devolviendo datos finales al bot: {datos_completos}")
        return datos_completos
    return None

def get_aemet_actual(concello):
    """Consulta InfluxDB para obtener los últimos datos de AEMET (como respaldo)"""
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()

    logging.info(f"🔍 [AEMET] Iniciando búsqueda de respaldo para: {concello}")

    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r["_measurement"] == "aemet_actual")
      |> filter(fn: (r) => r["estacion"] =~ /(?i){concello}/)
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    logging.info(f"📄 Query AEMET a ejecutar:\n{query}")

    try:
        result = query_api.query(query)
        logging.info(f"✅ Respuesta Query AEMET: {len(result)} tablas encontradas.")
        
        if result and len(result) > 0:
            rec = result[0].records[0] # <--- ¡CORREGIDO AQUÍ TAMBIÉN!
            logging.info(f"📊 Datos crudos extraídos de AEMET: {rec.values}")
            return {
                'temperature': rec.values.get('temperatura', 'N/A'),
                'relative_humidity': rec.values.get('humedad', 'N/A'),
                'wind_module': rec.values.get('viento_velocidad', 'N/A'),
                'ica': 'No medido por AEMET',
                'fuente': 'AEMET'
            }
        else:
            logging.warning(f"⚠️ AEMET devolvió vacío para {concello}.")
        return None
    except Exception as e:
        logging.error(f"Error Influx AEMET: {e}")
        return None
    finally:
        client.close()
# --- MANEJADORES DE COMANDOS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mensaje = (
        "👋 ¡Hola! Soy tu asistente meteorológico de Galicia.\n\n"
        "📌 *Comandos:*\n"
        "/suscribir `CONCELLO` - Recibe alertas\n"
        "/cancelar `CONCELLO` - Quitar suscripción\n"
        "/estado - Tus suscripciones\n"
        "/concellos - Lista de municipios\n"
        "/resumen `CONCELLO` - Tiempo actual"
    )
    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def suscribir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Di el concello. Ej: `/suscribir VIGO`", parse_mode='Markdown')
        return

    concello = " ".join(context.args).upper()
    chat_id = update.message.chat_id

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO usuarios (chat_id) VALUES (?)", (chat_id,))
    c.execute("REPLACE INTO suscripciones (chat_id, concello) VALUES (?, ?)", (chat_id, concello))
    conn.commit()
    conn.close()

    await update.message.reply_text(f"✅ Suscrito a *{concello}*.", parse_mode='Markdown')

async def estado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT concello FROM suscripciones WHERE chat_id = ?", (chat_id,))
    rows = c.fetchall()
    conn.close()

    if rows:
        lista = "\n- ".join([r[0] for r in rows])
        await update.message.reply_text(f"📊 *Estás suscrito a:*\n- {lista}", parse_mode='Markdown')
    else:
        await update.message.reply_text("No tienes suscripciones activas.")

async def cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ ¿Qué concello quieres cancelar?")
        return
    concello = " ".join(context.args).upper()
    chat_id = update.message.chat_id
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM suscripciones WHERE chat_id = ? AND concello = ?", (chat_id, concello))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"✅ Suscripción a *{concello}* eliminada.")

async def concellos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with open(CSV_PATH, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            lista = [row['Nombre_Concello'] for row in reader]
            txt = "📍 *Algunos concellos:* (Total: " + str(len(lista)) + ")\n- " + "\n- ".join(lista[:30])
            await update.message.reply_text(txt, parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text("❌ Error al leer la lista de concellos.")

async def resumen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ Ej: `/resumen VIGO`")
        return

    concello = " ".join(context.args).upper()
    datos = get_meteo_actual(concello)
    
    if not datos:
        datos = get_aemet_actual(concello) #si no hay datos de meteo pues intentamos de la aemet

    if datos:
        temp = datos.get('temperature', 'N/A')
        hum = datos.get('relative_humidity', 'N/A')
        # Recuerda que lo guardamos como wind_module
        velocidadViento = datos.get('wind_module', 'N/A')
        ica = datos.get('ica', 'N/A')

        # Hacemos que el bot muestre "Escribiendo..." en Telegram
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

        # Pedimos a la IA que redacte el consejo
        consejo_ia = obtener_consejo_ia(concello, temp, hum, velocidadViento, ica)

        mensaje = (
            f"📍 *El tiempo en {concello}:*\n\n"
            f"🌡 Temperatura: {temp}ºC\n"
            f"💧 Humedad: {hum}%\n"
            f"💨 Viento: {velocidadViento} m/s\n"
            f"🍃 Calidad Aire (ICA): {ica}\n\n"
            f"🤖 _Consejo:_\n{consejo_ia}"
        )
    else:
        mensaje = f"❌ No tengo datos recientes para *{concello}*."

    await update.message.reply_text(mensaje, parse_mode='Markdown')

async def enviar_alertas_automaticas(context: ContextTypes.DEFAULT_TYPE):
    """Revisa si hay mal tiempo y envía alertas a los suscritos."""
    logging.info("Iniciando revisión automática de alertas...")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT concello FROM suscripciones")
    concellos_activos = [row[0] for row in c.fetchall()]
    
    for concello in concellos_activos:
        datos = get_meteo_actual(concello)
        if not datos:
            continue
            
        temp = datos.get('temperature', 20) # si no saca datos pues usar unos por defecto que se consideren noramles para que no salten las alarmas
        hum = datos.get('relative_humidity', 50)
        viento = datos.get('wind_module', 0)
        ica = datos.get('ica', 'Desconocido')
        
        alerta = False
        motivo = ""
        
        # ⚠️ DEFINIMOS LOS UMBRALES DE PELIGRO
        if float(viento) > 40: 
            alerta = True; motivo = "fuertes rachas de viento"
        elif float(temp) < 5:
            alerta = True; motivo = "riesgo de heladas"
        elif float(hum) > 95:
            alerta = True; motivo = "alta probabilidad de precipitaciones/niebla"
            
        if alerta:
            consejo_ia = obtener_consejo_ia(concello, temp, hum, viento, ica)
            
            mensaje = (
                f"⚠️ *ALERTA METEOROLÓGICA* ⚠️\n\n"
                f"Detectadas *{motivo}* en {concello}.\n"
                f"🌡 Temp: {temp}ºC | 💨 Viento: {viento}\n\n"
                f"🤖 _Consejo:_ {consejo_ia}"
            )
            
            c.execute("SELECT chat_id FROM suscripciones WHERE concello = ?", (concello,))
            usuarios = c.fetchall()
            
            for (chat_id,) in usuarios:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=mensaje, parse_mode='Markdown')
                except Exception as e:
                    logging.error(f"Fallo al avisar al usuario {chat_id}: {e}")

    conn.close()


if __name__ == '__main__':
    print("Iniciando sistema...")
    init_db()
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("suscribir", suscribir))
    app.add_handler(CommandHandler("estado", estado))
    app.add_handler(CommandHandler("cancelar", cancelar))
    app.add_handler(CommandHandler("concellos", concellos))
    app.add_handler(CommandHandler("resumen", resumen))

    # Programa la revisión cada 10 *60 segundos (10 minutos)
    app.job_queue.run_repeating(enviar_alertas_automaticas, interval=600, first=10)
    print("🤖 Bot conectado y escuchando comandos...")
    app.run_polling()
