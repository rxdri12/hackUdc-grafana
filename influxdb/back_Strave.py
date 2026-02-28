import os
import sqlite3
import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

# Inicializamos solo la API web
app = FastAPI(title="Strava OAuth Callback API")

# Constantes (Asegúrate de pasar el SECRET en tu docker-compose o .env)
STRAVA_CLIENT_ID = "206827"
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET", "PON_AQUI_TU_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8736884501:AAH2e1BuT3FkKDTami4Cpjs7PGe-0HNl0_w")
DB_PATH = "/app/data/usuarios.db"

@app.get("/api/strava/callback", response_class=HTMLResponse)
async def strava_callback(code: str, state: str, error: str = None):
    # 1. Manejo del rechazo por parte del usuario
    if error:
        return f"<h1>Autenticación cancelada</h1><p>Motivo: {error}</p>"

    # 2. Intercambio del 'code' por el 'access_token' (Aquí sí hacemos un POST a Strava)
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code"
    }
    
    response = requests.post("https://www.strava.com/oauth/token", data=payload)
    
    if response.status_code != 200:
        return f"<h1>Error en Strava</h1><p>No se pudo obtener el token: {response.text}</p>"
        
    token_data = response.json()
    access_token = token_data.get("access_token")

    # 3. Persistencia en SQLite (Ojo al timeout)
    conn = None
    try:
        # Añadimos un timeout alto (10s). Si el contenedor del bot de Telegram
        # está leyendo la BD en este milisegundo, FastAPI esperará en lugar de lanzar 'database is locked'.
        conn = sqlite3.connect(DB_PATH, timeout=10.0) 
        c = conn.cursor()
        
        # Usamos REPLACE para sobrescribir si el usuario se vuelve a loguear
        c.execute("INSERT OR REPLACE INTO strava (chat_id, token) VALUES (?, ?)", (state, access_token))
        conn.commit()
    except sqlite3.Error as e:
        return f"<h1>Error interno</h1><p>Fallo al guardar en base de datos: {str(e)}</p>"
    finally:
        if conn:
            conn.close()

    # 4. Notificación asíncrona al usuario vía Telegram API
    # Hacemos la petición REST directamente a Telegram porque aquí no tenemos el objeto 'context' del bot
    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(telegram_url, json={
        "chat_id": state,
        "text": "✅ *Cuenta de Strava vinculada correctamente.*\n\nYa puedo analizar tus rutas frente a la calidad del aire.",
        "parse_mode": "Markdown"
    })

    # 5. Cierre de la experiencia de usuario en el navegador
    html_content = """
    <html>
        <head><meta name="viewport" content="width=device-width, initial-scale=1"></head>
        <body style="font-family: sans-serif; text-align: center; padding-top: 50px; background-color: #f4f4f9;">
            <h1 style="color: #fc4c02;">¡Strava Conectado!</h1>
            <p>Ya puedes cerrar esta pestaña y volver a la app de Telegram.</p>
        </body>
    </html>
    """
    return html_content