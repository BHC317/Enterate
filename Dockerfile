# Base Python
FROM python:3.11-slim

WORKDIR /app

# Dependencias de sistema
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    wget \
    unzip \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Copiar y instalar requirements de la API
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copiar y instalar requirements del ETL
COPY etl/infra/requirements.txt /app/etl/requirements.txt
RUN pip install --no-cache-dir -r /app/etl/requirements.txt

# Copiar código de la API y del ETL
COPY app /app
COPY etl /app/etl

# Exponer puerto de la API
EXPOSE 8001
ENV TZ="Europe/Madrid"

# Comando por defecto: arrancar la API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
