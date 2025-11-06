#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
worker.py
---------
Microservice scraper_liste_html :
- Télécharge la liste complète des véhicules depuis ERA (XML)
- Convertit le XML en CSV journalier
- Envoie un message unique à RabbitMQ pour notifier que le CSV est prêt
"""

# --------------------------------------
# Importations de modules
# --------------------------------------
import os                   # Pour manipuler les chemins et variables d'environnement
import time                 # Pour gérer les délais et sleep
import socket               # Pour gérer certaines erreurs réseau lors de la connexion RabbitMQ
from datetime import datetime  # Pour récupérer la date du jour pour les CSV
import xml.etree.ElementTree as ET  # Pour parser le fichier XML
import pandas as pd         # Pour manipuler et créer des CSV
import pika                 # Client RabbitMQ pour Python
from playwright.sync_api import sync_playwright  # Pour automatiser la navigation web et téléchargement

# --------------------------------------
# Configuration générale
# --------------------------------------
BASE_URL = "https://eratv.era.europa.eu"                  # URL de base du site ERA
LIST_URL = f"{BASE_URL}/Eratv/Home/List"                 # URL de la page listant les véhicules

# Répertoires locaux pour stocker les CSV et les téléchargements temporaires
DATA_DIR = os.getenv("DATA_DIR", "/app/data/csv_listes")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/app/data/downloads")
TEMP_FILE = "export_temp.xml"                            # Nom temporaire pour le fichier XML téléchargé

# Création des répertoires s'ils n'existent pas
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Variables pour la connexion RabbitMQ (via Docker Compose)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "csv_list.queue")

# --------------------------------------
# Fonction : connexion à RabbitMQ avec retry
# --------------------------------------
def connect_rabbitmq():
    """
    Tente de se connecter à RabbitMQ plusieurs fois si nécessaire.
    Retourne la connection et le channel.
    """
    max_retries = 10
    retry_delay = 5
    for attempt in range(1, max_retries + 1):
        try:
            # Création des credentials
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

            # Tentative de connexion
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    credentials=credentials,
                    heartbeat=600,  # pour éviter la déconnexion si inactif
                    blocked_connection_timeout=300  # timeout pour bloqué si broker lent
                )
            )

            # Création du channel et déclaration de la queue durable
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)

            print(f"[INFO] Connexion RabbitMQ réussie après {attempt} tentative(s)")
            return connection, channel

        except (pika.exceptions.AMQPConnectionError, socket.gaierror, pika.exceptions.ChannelClosedByBroker) as e:
            print(f"[WARN] RabbitMQ non disponible, retry {attempt}/{max_retries} dans {retry_delay}s... ({e})")
            time.sleep(retry_delay)

    # Si aucune tentative n'a fonctionné
    raise Exception("Impossible de se connecter à RabbitMQ après plusieurs tentatives")

# --------------------------------------
# Fonction : téléchargement du XML via Playwright
# --------------------------------------
def download_xml_playwright():
    """
    Automatise le navigateur pour :
    1. Accéder à la page List ERA
    2. Cocher tous les pays
    3. Sélectionner l'export XML
    4. Télécharger le fichier XML
    Retourne le chemin complet du fichier téléchargé.
    """
    with sync_playwright() as p:
        # Lancement du navigateur Chromium headless
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        print("⬇️ Ouverture de la page List ERA...")
        page.goto(LIST_URL)

        # Cocher tous les pays disponibles pour le filtre
        checkboxes = page.query_selector_all("div.GroupChecks.HorChecks input[type='checkbox']")
        print(f"⬇️ {len(checkboxes)} pays trouvés, on coche tout...")
        for cb in checkboxes:
            if not cb.is_checked():
                cb.check()

        # Sélection de l'option Export XML
        page.select_option("#ExportList", "2")  # '2' correspond à XML

        # Cliquer sur OK pour déclencher le téléchargement
        with page.expect_download() as download_info:
            page.click("#btnExport")

        download = download_info.value
        file_path = os.path.join(DOWNLOAD_DIR, TEMP_FILE)
        download.save_as(file_path)
        print(f"✅ Fichier XML téléchargé dans : {file_path}")

        # Fermeture du navigateur
        browser.close()
        return file_path

# --------------------------------------
# Fonction : parser le XML et générer un CSV
# --------------------------------------
def parse_xml_to_csv(file_path):
    """
    Lit le fichier XML téléchargé, extrait les informations pertinentes
    et génère un CSV journalier dans DATA_DIR.
    """
    print("📖 Lecture et parsing du fichier XML...")
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Extraction des informations de chaque résultat
    rows = []
    for item in root.findall(".//Result"):
        rows.append({
            "Type_ID": item.attrib.get("Type_ID", "").strip() or None,
            "Authorisation_document_reference (EIN)": item.attrib.get(
                "Authorisation_document_reference__x0028_EIN_x0029_", ""
            ).strip() or None,
            "Type_Name": item.attrib.get("Type_Name", "").strip() or None,
            "Authorisation_Status": item.attrib.get("Authorisation_Status", "").strip() or None,
            "Last_Update": item.attrib.get("Last_Update", "").strip() or None,
        })

    # Création du DataFrame Pandas
    df = pd.DataFrame(rows)

    # Génération des URLs ERA pour chaque Type_ID
    df["ERA_URL"] = df["Type_ID"].astype(str).apply(
        lambda x: f"{BASE_URL}/Eratv/Home/View/{x}" if pd.notna(x) and x else None
    )

    # Sauvegarde du CSV avec la date du jour
    today = datetime.now().strftime("%Y-%m-%d")
    csv_path = os.path.join(DATA_DIR, f"liste_vehicules_{today}.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"✅ CSV sauvegardé : {csv_path}")
    return csv_path

# --------------------------------------
# Fonction : envoyer un message RabbitMQ
# --------------------------------------
def send_csv_ready_message(channel, csv_file):
    """
    Publie un message sur RabbitMQ pour indiquer que le CSV du jour est prêt.
    Retry 5 fois en cas d'erreur.
    """
    message = f"CSV ready: {csv_file}"
    for attempt in range(5):
        try:
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)  # message persistant
            )
            print(f"[INFO] Message envoyé à RabbitMQ: {message}")
            break
        except pika.exceptions.AMQPConnectionError:
            print(f"[WARN] Erreur d'envoi, retry {attempt + 1}/5...")
            time.sleep(2)
    else:
        raise Exception("Impossible d'envoyer le message à RabbitMQ")

# --------------------------------------
# Main
# --------------------------------------
def main():
    # Connexion RabbitMQ
    connection, channel = connect_rabbitmq()
    try:
        # Téléchargement du XML via Playwright
        xml_path = download_xml_playwright()

        # Parsing du XML et création du CSV
        csv_file = parse_xml_to_csv(xml_path)

        # Envoi message RabbitMQ pour signaler la disponibilité du CSV
        send_csv_ready_message(channel, csv_file)
    finally:
        # Fermeture de la connexion RabbitMQ
        connection.close()
        print("[INFO] Worker terminé avec succès.")

# Exécution du script
if __name__ == "__main__":
    main()
