#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parser_liste_csv/worker.py
--------------------------
Lit le CSV généré par scraper_liste_html, compare avec la table PostgreSQL 'liste_ERATV',
détecte les nouveaux ou modifiés, met à jour la base via UPSERT, et envoie les URLs
dans la queue 'vehicle_pages.queue' pour le scraping détaillé.
"""

import os
import time
import re
import pandas as pd
import psycopg2
import pika

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_IN = os.getenv("QUEUE_IN", "csv_list.queue")
QUEUE_OUT = os.getenv("QUEUE_OUT", "vehicle_pages.queue")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "DB_ERATV")
DB_USER = os.getenv("DB_USER", "guest")
DB_PASS = os.getenv("DB_PASS", "guest")

CSV_DIR = "/app/data/csv_listes"

# ------------------------------------------------------------
# Connexion PostgreSQL
# ------------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

# ------------------------------------------------------------
# Connexion RabbitMQ
# ------------------------------------------------------------
def get_rabbit_channel():
    for attempt in range(10):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600)
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_OUT, durable=True)
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print(f"[WARN] RabbitMQ non dispo, retry {attempt+1}/10")
            time.sleep(5)
    raise Exception("Impossible de se connecter à RabbitMQ")

# ------------------------------------------------------------
# Extraction du chemin CSV depuis le message
# ------------------------------------------------------------
def parse_csv_path(message: str) -> str:
    match = re.search(r"CSV ready:\s*(.*)", message)
    return match.group(1).strip() if match else None

# ------------------------------------------------------------
# Comparaison CSV vs DB et mise à jour avec UPSERT
# ------------------------------------------------------------
def compare_and_update(csv_path, db_conn, rabbit_channel):
    print(f"[INFO] Lecture du CSV : {csv_path}")
    # Lecture CSV et remplissage des NaN
    df_csv = pd.read_csv(csv_path, dtype=str).fillna("")
    # Normalisation des noms de colonnes pour éviter les erreurs
    df_csv.columns = [c.strip() for c in df_csv.columns]

    urls_to_publish = []

    with db_conn.cursor() as cur:
        for _, row in df_csv.iterrows():
            csv_rec = row.to_dict()
            type_id = csv_rec.get("TypeID", "").strip()
            ein = csv_rec.get("EIN", "").strip()
            name = csv_rec.get("TypeName", "").strip()
            status = csv_rec.get("Status", "").strip()
            last_update = csv_rec.get("LastUpdate", "").strip()
            url_tv = csv_rec.get("URL", "").strip()

            if not type_id:
                continue  # ignorer les lignes sans TypeID

            # UPSERT : insert ou update si Type_ID existe déjà
            cur.execute("""
                INSERT INTO liste_ERATV
                (Type_ID, Authorisation_document_reference_EIN, Type_Name,
                 Authorisation_Status, Last_Update, URL_TV)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (Type_ID) DO UPDATE SET
                    Authorisation_document_reference_EIN = EXCLUDED.Authorisation_document_reference_EIN,
                    Type_Name = EXCLUDED.Type_Name,
                    Authorisation_Status = EXCLUDED.Authorisation_Status,
                    Last_Update = EXCLUDED.Last_Update,
                    URL_TV = EXCLUDED.URL_TV,
                    updated_at = NOW()
            """, (type_id, ein, name, status, last_update, url_tv))

            urls_to_publish.append(url_tv)
            print(f"[INFO] Traité : {type_id}")

    db_conn.commit()
    print(f"[INFO] {len(urls_to_publish)} URLs à publier")

    # Publication des URLs dans RabbitMQ
    for url in urls_to_publish:
        rabbit_channel.basic_publish(
            exchange="",
            routing_key=QUEUE_OUT,
            body=url,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[PUBLISHED] URL envoyée: {url}")

# ------------------------------------------------------------
# Callback RabbitMQ
# ------------------------------------------------------------
def on_message(ch, method, properties, body):
    msg = body.decode("utf-8").strip()
    print(f"[INFO] Message reçu : {msg}")

    csv_path = parse_csv_path(msg)
    if not csv_path or not os.path.exists(csv_path):
        print(f"[WARN] Fichier CSV introuvable : {csv_path}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    conn = get_db_connection()
    _, out_channel = get_rabbit_channel()

    try:
        compare_and_update(csv_path, conn, out_channel)
    finally:
        conn.close()
        out_channel.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)
    print("[INFO] Traitement terminé pour le message.")

# ------------------------------------------------------------
# Programme principal
# ------------------------------------------------------------
def main():
    print("[INFO] Démarrage du worker parser_liste_csv")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_IN, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_IN, on_message_callback=on_message)
    print(f"[INFO] En attente de messages dans {QUEUE_IN} ...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
