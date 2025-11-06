# Utiliser une image Python légère
FROM python:3.11-slim

# Variables d'environnement pour éviter la mise en cache de pycache
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Installer les dépendances système
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Créer le répertoire de l'application
WORKDIR /app

# Copier requirements
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code
COPY . .

# Commande par défaut pour lancer le worker
CMD ["python", "worker.py"]
