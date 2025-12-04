# --------------------------------------
# Image de base
# --------------------------------------
# Utilise l'image officielle Python avec Playwright préinstallé
# v1.44.0-focal correspond à une version stable sur Ubuntu 20.04
FROM mcr.microsoft.com/playwright/python:v1.44.0-focal

# --------------------------------------
# Définition du répertoire de travail
# --------------------------------------
WORKDIR /app

# --------------------------------------
# Installation des dépendances Python
# --------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------
# Copier le script worker dans le conteneur
# --------------------------------------
COPY worker.py .

# --------------------------------------
# Création des répertoires pour les fichiers
# --------------------------------------
ENV DATA_DIR=/app/data/xml_types_vehicules
RUN mkdir -p $DATA_DIR

# --------------------------------------
# Définition du point d'entrée du conteneur
# --------------------------------------
CMD ["python", "worker.py"]
