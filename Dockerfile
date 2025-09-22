# FROM python:3.12-slim
# WORKDIR /code
# COPY requirements.txt requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt
# COPY . /code
FROM python:3.11-slim

WORKDIR /app

# Instala dependencias del sistema para psycopg2
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app
EXPOSE 8001
ENV TZ="Europe/Madrid"
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
