#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
worker.py
---------
Scraper la liste complète des véhicules depuis ERA (HTML), page par page.
- Sauvegarde les CSV quotidiens avec la date du jour.
- Envoie un message unique à RabbitMQ pour indiquer que le CSV du jour est prêt.
"""

import os
import time
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pika  # Pour RabbitMQ

# --------------------------------------
# Configuration
# --------------------------------------

BASE_URL = "https://eratv.era.europa.eu/Eratv/Home/List"
DATA_DIR = os.getenv("DATA_DIR", "/app/data/csv_listes")
os.makedirs(DATA_DIR, exist_ok=True)

# RabbitMQ : récupération des variables d'environnement
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "csv_list.queue")

# --------------------------------------
# Initialisation Selenium
# --------------------------------------

chrome_options = Options()
chrome_options.add_argument("--headless")  # Exécution en mode headless
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.binary_location = "/usr/bin/chromium"  # chemin du binaire chromium

driver = webdriver.Chrome(options=chrome_options)

# --------------------------------------
# Connexion RabbitMQ
# --------------------------------------

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
)
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# --------------------------------------
# Fonctions
# --------------------------------------

def extract_vehicle_data():
    """
    Parcours la page HTML pour récupérer les informations des véhicules.
    Retourne une liste de dictionnaires avec:
    TypeID, EIN, TypeName, Status, LastUpdate, URL
    """
    data = []
    rows = driver.find_elements(By.CSS_SELECTOR, "table.Tabular tbody tr")
    for row in rows:
        try:
            link_element = row.find_element(By.TAG_NAME, "a")
            type_id = link_element.text.strip()
            url = link_element.get_attribute("href")

            cells = row.find_elements(By.TAG_NAME, "td")
            ein = cells[1].text.strip()
            type_name = cells[2].text.strip()
            status = cells[3].text.strip()
            last_update = cells[4].text.strip()

            data.append({
                "TypeID": type_id,
                "EIN": ein,
                "TypeName": type_name,
                "Status": status,
                "LastUpdate": last_update,
                "URL": url
            })

        except Exception as e:
            print(f"[WARN] Impossible de traiter une ligne: {e}")

    return data

def save_csv(data):
    """
    Sauvegarde la liste complète dans un CSV daté du jour.
    """
    df = pd.DataFrame(data)
    today = datetime.now().strftime("%Y-%m-%d")  # Date du jour
    filename = os.path.join(DATA_DIR, f"liste_vehicules_{today}.csv")
    df.to_csv(filename, index=False, encoding="utf-8")
    print(f"[INFO] Liste complète sauvegardée dans {filename}")
    return filename

def send_csv_ready_message(csv_file):
    """
    Envoie un message unique à RabbitMQ pour indiquer que le CSV du jour est prêt.
    L'autre microservice pourra le parser et comparer avec la DB.
    """
    message = f"CSV ready: {csv_file}"
    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # message persistant
        )
    )
    print(f"[INFO] Message envoyé à RabbitMQ: {message}")

# --------------------------------------
# Exécution principale avec pagination
# --------------------------------------

def main():
    driver.get(BASE_URL)
    time.sleep(3)  # attendre le JS

    vehicle_data = []  # Liste complète de tous les véhicules

    # -----------------------
    # Récupérer le nombre total d'éléments et configurer la pagination
    # -----------------------
    total_items = int(driver.find_element(By.CSS_SELECTOR, "div.Pagination p.Matches strong").text)
    
    # Forcer la page size à 100 via JS pour réduire le nombre de pages
    driver.execute_script("document.getElementById('PagedGrid_PageSize').value='100';"
                          "document.getElementById('PagedGrid_CurrentPage').value='1';"
                          "document.forms['SearchForm'].submit();")
    time.sleep(3)  # attendre le chargement après changement de page_size

    # Récupérer le nouveau page size (100)
    page_size = int(driver.find_element(By.ID, "PagedGrid_PageSize").get_attribute("value"))
    total_pages = (total_items + page_size - 1) // page_size  # arrondi vers le haut

    print(f"[INFO] Total véhicules: {total_items}, pages: {total_pages}, éléments par page: {page_size}")

    # -----------------------
    # Parcours de toutes les pages
    # -----------------------
    for page in range(1, total_pages + 1):
        print(f"[INFO] Récupération page {page}/{total_pages}")

        if page > 1:
            # Naviguer vers la page suivante via JS et soumettre le formulaire
            driver.execute_script(
                f"document.getElementById('PagedGrid_CurrentPage').value='{page}';"
                "document.forms['SearchForm'].submit();"
            )
            time.sleep(2)  # attendre le chargement

        # Extraction des données de la page courante
        page_data = extract_vehicle_data()
        vehicle_data.extend(page_data)
        print(f"[INFO] {len(page_data)} véhicules récupérés sur cette page")

    # -----------------------
    # Sauvegarde CSV complet
    # -----------------------
    csv_file = save_csv(vehicle_data)

    # -----------------------
    # Envoi d'un seul message à RabbitMQ
    # -----------------------
    send_csv_ready_message(csv_file)

    # -----------------------
    # Fermeture des ressources
    # -----------------------
    driver.quit()
    connection.close()
    print("[INFO] Worker terminé. Liste complète traitée et message RabbitMQ envoyé.")

# --------------------------------------
# Lancement
# --------------------------------------

if __name__ == "__main__":
    main()
