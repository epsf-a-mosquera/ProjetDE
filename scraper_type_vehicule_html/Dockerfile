# --------------------------------------
# Image de base
# --------------------------------------
# Utilise Python 3.11 officiel sur Ubuntu 22.04
FROM python:3.11-slim

# --------------------------------------
# Définition du répertoire de travail
# --------------------------------------
WORKDIR /app

# --------------------------------------
# Copie et installation des dépendances
# --------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------
# Copie du script worker
# --------------------------------------
COPY worker.py .

# --------------------------------------
# Création du dossier pour les fichiers XML
# --------------------------------------
ENV DATA_DIR=/app/data/xml_types_vehicules
RUN mkdir -p $DATA_DIR

# --------------------------------------
# Point d'entrée du conteneur
# --------------------------------------
CMD ["python", "worker.py"]
