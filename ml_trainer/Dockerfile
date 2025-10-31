FROM python:3.11-slim
WORKDIR /app

# Copiamos las dependencias y las instalamos.
COPY ../../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el archivo Python principal del servicio.
COPY ./train.py .

# Ejecutamos directamente el script al iniciar el contenedor.
CMD ["python", "train.py"]
