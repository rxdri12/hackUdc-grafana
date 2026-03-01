# - Centro de Control Inteligente Metereológico

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://www.docker.com/)
[![Grafana](https://img.shields.io/badge/Grafana-Dashboard-orange.svg)](https://grafana.com/)
[![HackUDC 2026](https://img.shields.io/badge/HackUDC-2026-purple.svg)]()

> Proyecto desarrollado para el **HackUDC 2026**. Un sistema integral de monitorización en tiempo real que cruza datos meteorológicos extremos con incidencias en Galicia para prevenir accidentes, incluye imágenes actuales (se renuevan cada 10 min) sobre varios puntos de galicia, alertas por temperaturas extremas (altas o bajas), ráfagas de viento extremos, top 10 temperaturas, calidad medioambiental y un increíble chatbot que usa inteligencia artificial para viendo los datos del tiempo del lugar que le pides, generar unas recomendaciones y una explicación sencilla e intuitiva del tiempo.

## 🚀 ¿Qué hace este proyecto?

Este sistema recolecta, almacena y visualiza en tiempo real datos críticos del tiempo gallegas. Combina fuentes meteorológicas (MeteoGalicia, AEMET)para crear mapas de calor, paneles de control y **alertas automatizadas con capturas de pantalla** a través de Telegram.

### ✨ Características Principales

* 🗺️ **Mapa Interactivo (Geomap):** Visualización en tiempo real de cámaras de Meteogalicia y estaciones meteorológicas en verde.
* 🚨 **Sistema de Alertas Inteligente:** Si detecta vientos huracanados, heladas extremas o incidencias críticas, el bot de Telegram envía una alerta instantánea.
* 📸 **Capturas Automáticas:** Las alertas de Telegram incluyen un *snapshot* (foto) generado en tiempo real del panel exacto de Grafana gracias a un motor de renderizado interno.
* 🔓 **Acceso Público (Modo Kiosko):** Los paneles están configurados con autenticación anónima para que cualquier ciudadano (o el jurado) pueda consultar el estado sin la necesidad de registrarse.

## 🏗️ Arquitectura y Tecnologías

El proyecto sigue una arquitectura profesional basada en microservicios contenerizados:

* **Python 3:** Scripts recolectores (`cron`/`schedule`) que consumen las APIs de MeteoGalicia y Aemet.
* **InfluxDB (Time-Series DB):** Base de datos optimizada para almacenar coordenadas, temperaturas, vientos e incidencias en el tiempo.
* **Grafana + Image Renderer:** Interfaz visual y motor de alertas con renderizado de imágenes.
* **Docker Compose:** Orquestación de todos los servicios en una red privada segura (`vaultwarden_web`).
* **Ollama:** modelo LLM de inteligencia artificial para el uso de las recomendaciones del chatbot.

## ⚙️ Instalación y Despliegue

Nosotros mismos hemos hecho uso de un dominio propio y hemos levantado todo en una raspberry pi 5 8GB, cuando decimos todo es todo, desde los recopiladores de información (los ejecutables python) a grafana, bases de datos , bot de telegram y el modelo LLM Ollama.

¡Levantar el proyecto es cuestión de segundos!

1. **Clonar el repositorio:**
   ```bash
   git clone [https://github.com/rxdri12/hackUdc-grafana.git](https://github.com/rxdri12/hackUdc-grafana.git)
   cd hackUdc-grafana
```

2. **Configurar en un .env los tokens de las APIs de meteogalicia, Aemet y el token del bot de telegram**

3. **Levantar el proyecto:**
    ```bash
    cd grafana
    docker compose up -d
    cd ../influxdb
    docker compose up -d --build
    ```

4. **Resultado:**
    Tendra su bot de telegram preparado (según el nombre que le haya dado al crearlo), en grafana.sudominio.es podrá ver los dahsboards.

