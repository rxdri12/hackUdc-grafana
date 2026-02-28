FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ejecutamos con -u para ver los logs de Python en tiempo real en Docker
CMD ["python", "-u", "bot.py"]
