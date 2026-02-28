FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY concellos.csv .
COPY recolector.py .
COPY recolector_ica.py .
COPY recolector_aemet.py .
COPY recolector_limpia_datos.py .
