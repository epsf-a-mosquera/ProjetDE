FROM python:3.11-slim
WORKDIR /app

# Copia dependencias y las instala
COPY ../../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código de la API
COPY ./app ./app

# Expone el puerto de la API
EXPOSE 8500

# Comando para iniciar la API
CMD ["uvicorn", "app.predict:app", "--host", "0.0.0.0", "--port", "8500"]
