#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import pika
import socket
from playwright.sync_api import sync_playwright

# -----------------------------
# Configuration via ENV
# -----------------------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_IN = os.getenv("QUEUE_IN", "vehicle_pages.queue")
QUEUE_OUT = os.getenv("QUEUE_OUT", "xml_vehicle.queue")  # queue de sortie vers parser_type_vehicule_xml
DATA_DIR = os.getenv("DATA_DIR", "/app/data/xml_types_vehicules")

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
# Fonction de scrap XML avec Playwright
# -----------------------------
def scrape_xml(vehicle_id, channel_out):
    base_url = "https://eratv.era.europa.eu"
    vehicle_url = f"{base_url}/Eratv/Home/View/{vehicle_id}"
    print(f"[INFO] Ouverture de la page HTML : {vehicle_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto(vehicle_url, wait_until="domcontentloaded")  # attendre que DOM soit chargé

        # Attendre que le container principal soit visible
        page.wait_for_selector("div#Container div#Content div[style*='float:right']", timeout=15000)

        # Récupérer le lien XML
        xml_link_tag = page.query_selector("div#Container div#Content div[style*='float:right'] a[href*='exportTo=XML']")
        if not xml_link_tag:
            browser.close()
            raise Exception(f"Lien XML non trouvé pour vehicle_id={vehicle_id}")

        xml_url = xml_link_tag.get_attribute("href")
        if not xml_url.startswith("http"):
            xml_url = base_url + xml_url

        print(f"[INFO] Téléchargement du fichier XML : {xml_url}")

        # Télécharger le XML
        with page.expect_download() as download_info:
            page.click(f"a[href='{xml_link_tag.get_attribute('href')}']")
        download = download_info.value
        file_path = os.path.join(DATA_DIR, f"{vehicle_id}.xml")
        download.save_as(file_path)

        print(f"[INFO] Fichier XML sauvegardé : {file_path}")

        # Publier le fichier XML dans la queue xml_vehicle.queue
        channel_out.basic_publish(
            exchange='',
            routing_key=QUEUE_OUT,
            body=file_path,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[INFO] Message publié dans {QUEUE_OUT} : {file_path}")

        browser.close()

# -----------------------------
# Callback RabbitMQ
# -----------------------------
def callback(ch, method, properties, body, channel_out):
    vehicle_id = body.decode().strip()
    try:
        scrape_xml(vehicle_id, channel_out)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[ERROR] Erreur scrap XML pour {vehicle_id} : {e}")
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
