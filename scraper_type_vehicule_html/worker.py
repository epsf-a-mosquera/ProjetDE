#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Worker scraper_type_vehicule_html :
- Consomme vehicle_pages.queue (liste des URLs ERA pour chaque Type_ID)
- Télécharge le XML associé
- Publie le chemin du XML dans xml_vehicle.queue
"""

import os
import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pika
import socket
from datetime import datetime

# -----------------------------
# Configuration via ENV
# -----------------------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_IN = os.getenv("QUEUE_IN", "vehicle_pages.queue")
QUEUE_OUT = os.getenv("QUEUE_OUT", "xml_vehicle.queue")
DATA_DIR = os.getenv("DATA_DIR", "/app/data/xml_types_vehicules")

BASE_URL = "https://eratv.era.europa.eu"

os.makedirs(DATA_DIR, exist_ok=True)

# -----------------------------
# Connexion RabbitMQ avec retry
# -----------------------------
def connect_rabbitmq():
    max_retries = 10
    retry_delay = 5
    for attempt in range(1, max_retries + 1):
        try:
            creds = pika.PlainCredentials("guest", "guest")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=creds, heartbeat=600)
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_IN, durable=True)
            channel.queue_declare(queue=QUEUE_OUT, durable=True)
            print(f"[INFO] Connexion RabbitMQ réussie (tentative {attempt})")
            return connection, channel
        except (pika.exceptions.AMQPConnectionError, socket.gaierror) as e:
            print(f"[WARN] RabbitMQ non disponible (retry {attempt}/{max_retries}): {e}")
            time.sleep(retry_delay)
    raise Exception("Impossible de se connecter à RabbitMQ")

# -----------------------------
# Fonction safe_get avec retry
# -----------------------------
def safe_get(url, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            print(f"⚠️ Tentative {attempt}/{max_retries} échouée ({e.__class__.__name__}). Nouvelle tentative dans 5s...")
            time.sleep(5)
    raise ConnectionError(f"❌ Impossible d'accéder à {url} après {max_retries} tentatives.")

# -----------------------------
# Fonction pour scraper un XML depuis ERA
# -----------------------------
def scrape_xml(era_url, type_id):
    print(f"\n🚀 Téléchargement du XML pour Type ID : {type_id}")
    response = safe_get(era_url)
    soup = BeautifulSoup(response.text, "html.parser")

    xml_link_tag = None
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "exportTo=XML" in href:
            xml_link_tag = a_tag
            break

    if not xml_link_tag:
        raise Exception(f"Aucun lien XML trouvé pour {type_id}")

    xml_url = xml_link_tag["href"]
    if not xml_url.startswith("http"):
        xml_url = BASE_URL + xml_url
    print(f"✅ Lien XML trouvé : {xml_url}")

    xml_response = safe_get(xml_url)
    last_update = datetime.now().strftime("%Y-%m-%d")
    filename = f"{type_id}_{last_update}.xml".replace("/", "-")
    filepath = os.path.join(DATA_DIR, filename)

    with open(filepath, "wb") as f:
        f.write(xml_response.content)
    print(f"✅ Fichier XML sauvegardé : {filepath}")
    return filepath

# -----------------------------
# Callback RabbitMQ
# -----------------------------
def callback(ch, method, properties, body, channel_out):
    try:
        message = body.decode().strip()
        type_id, era_url = message.split("|", 1)  # on attend "Type_ID|ERA_URL"
        xml_path = scrape_xml(era_url, type_id)

        # Publier le fichier XML dans la queue de sortie
        channel_out.basic_publish(
            exchange='',
            routing_key=QUEUE_OUT,
            body=xml_path,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[INFO] Message publié dans {QUEUE_OUT} : {xml_path}")

        # Pause aléatoire pour éviter surcharge du site
        wait_time = random.randint(3*60, 7*60)
        print(f"⏱️ Pause de {wait_time//60} min {wait_time%60} sec avant le prochain téléchargement...")
        time.sleep(wait_time)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[ERROR] Erreur scrap XML : {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    connection, channel_in = connect_rabbitmq()
    channel_out = connection.channel()
    channel_out.queue_declare(queue=QUEUE_OUT, durable=True)

    channel_in.basic_qos(prefetch_count=1)
    channel_in.basic_consume(
        queue=QUEUE_IN,
        on_message_callback=lambda ch, method, properties, body: callback(ch, method, properties, body, channel_out),
        auto_ack=False
    )

    print("[INFO] En attente de nouveaux messages dans vehicle_pages.queue...")
    channel_in.start_consuming()
