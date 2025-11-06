# --------------------------------------
# Image de base
# --------------------------------------
# Utilise l'image officielle Python avec Playwright préinstallé
# v1.44.0-focal correspond à une version stable sur Ubuntu 20.04
FROM mcr.microsoft.com/playwright/python:v1.44.0-focal

# --------------------------------------
# Définition du répertoire de travail
# --------------------------------------
# Tous les chemins et commandes suivantes se feront à partir de /app
WORKDIR /app

# --------------------------------------
# Installation des dépendances Python
# --------------------------------------
# Copier le fichier requirements.txt dans le conteneur
COPY requirements.txt .

# Installer les dépendances listées dans requirements.txt
# --no-cache-dir empêche la mise en cache pour réduire la taille de l'image
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------
# Copier le script worker dans le conteneur
# --------------------------------------
# worker.py est le script principal du microservice scraper
COPY worker.py .

# --------------------------------------
# Création des répertoires pour les fichiers
# --------------------------------------
# Définition des variables d'environnement pour l'accès dans le script Python
ENV DATA_DIR=/app/data/csv_listes
ENV DOWNLOAD_DIR=/app/data/downloads

# Création physique des dossiers pour éviter les erreurs lors du téléchargement ou CSV
RUN mkdir -p $DATA_DIR $DOWNLOAD_DIR

# --------------------------------------
# Définition du point d'entrée du conteneur
# --------------------------------------
# Lorsque le conteneur démarre, il exécute le script worker.py
CMD ["python", "worker.py"]
