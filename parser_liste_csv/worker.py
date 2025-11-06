#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parser_liste_csv/worker.py
--------------------------
Consomme les CSV produits par scraper_liste_html,
synchronise la table liste_eratv dans PostgreSQL,
et publie les URLs nouvelles ou modifiées dans vehicle_pages.queue.
"""

import os
import time
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import pika
import socket
import functools

# ===============================================================
#  Configuration via variables d'environnement
# ===============================================================
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_IN = os.getenv("QUEUE_IN", "csv_list.queue")
QUEUE_OUT = os.getenv("QUEUE_OUT", "vehicle_pages.queue")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "DB_ERATV")
DB_USER = os.getenv("DB_USER", "guest")
DB_PASS = os.getenv("DB_PASS", "guest")

DATA_DIR = os.getenv("DATA_DIR", "/app/data/csv_listes")
os.makedirs(DATA_DIR, exist_ok=True)

# ===============================================================
#  Utilitaires
# ===============================================================
def normalize(val):
    """Normalise une valeur pour la comparaison (str, strip, vide->'')."""
    if pd.isna(val):
        return ""
    return str(val).strip()

# ===============================================================
#  Connexion RabbitMQ avec retry
# ===============================================================
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
            channel.basic_qos(prefetch_count=1)
            print(f"[INFO] Connexion RabbitMQ réussie (tentative {attempt})")
            return connection, channel
        except (pika.exceptions.AMQPConnectionError, socket.gaierror) as e:
            print(f"[WARN] RabbitMQ non disponible (retry {attempt}/{max_retries}): {e}")
            time.sleep(retry_delay)
    raise Exception("Impossible de se connecter à RabbitMQ")

# ===============================================================
#  Connexion PostgreSQL avec retry
# ===============================================================
def connect_postgres():
    max_retries = 10
    retry_delay = 5
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS
            )
            print(f"[INFO] Connexion PostgreSQL réussie (tentative {attempt})")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[WARN] PostgreSQL non disponible (retry {attempt}/{max_retries}): {e}")
            time.sleep(retry_delay)
    raise Exception("Impossible de se connecter à PostgreSQL")

# ===============================================================
#  Synchronisation du CSV avec la base
# ===============================================================
def process_csv(csv_file, db_conn, channel_out):
    file_path = os.path.join(DATA_DIR, os.path.basename(csv_file))
    print(f"[INFO] Traitement du fichier CSV : {file_path}")

    df = pd.read_csv(file_path)
    cursor = db_conn.cursor(cursor_factory=RealDictCursor)

    updated_count = 0
    inserted_count = 0

    for _, row in df.iterrows():
        type_id = normalize(row["Type_ID"])
        ein = normalize(row["Authorisation_document_reference (EIN)"])
        type_name = normalize(row["Type_Name"])
        status = normalize(row["Authorisation_Status"])
        last_update = normalize(row["Last_Update"])
        url_tv = normalize(row["ERA_URL"])

        cursor.execute("SELECT * FROM liste_eratv WHERE type_id = %s", (type_id,))
        existing = cursor.fetchone()

        if existing:
            changed = (
                normalize(existing["authorisation_document_reference_ein"]) != ein or
                normalize(existing["type_name"]) != type_name or
                normalize(existing["authorisation_status"]) != status or
                normalize(existing["last_update"]) != last_update or
                normalize(existing["url_tv"]) != url_tv
            )
            if changed:
                cursor.execute("""
                    UPDATE liste_eratv
                    SET authorisation_document_reference_ein=%s,
                        type_name=%s,
                        authorisation_status=%s,
                        last_update=%s,
                        url_tv=%s
                    WHERE type_id=%s
                """, (ein, type_name, status, last_update, url_tv, type_id))
                db_conn.commit()
                updated_count += 1
                channel_out.basic_publish(
                    exchange='',
                    routing_key=QUEUE_OUT,
                    body=url_tv,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
        else:
            cursor.execute("""
                INSERT INTO liste_eratv(type_id, authorisation_document_reference_ein,
                type_name, authorisation_status, last_update, url_tv)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (type_id, ein, type_name, status, last_update, url_tv))
            db_conn.commit()
            inserted_count += 1
            channel_out.basic_publish(
                exchange='',
                routing_key=QUEUE_OUT,
                body=url_tv,
                properties=pika.BasicProperties(delivery_mode=2)
            )

    cursor.close()
    print(f"[INFO] Synchronisation terminée : {inserted_count} insertions, {updated_count} mises à jour.")

# ===============================================================
#  Callback RabbitMQ
# ===============================================================
def callback(ch, method, properties, body, channel_out):
    csv_file = body.decode()
    print(f"[INFO] Nouveau message reçu : {csv_file}")

    try:
        db_conn = connect_postgres()
        process_csv(csv_file, db_conn, channel_out)
        db_conn.close()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[ERROR] Erreur lors du traitement du CSV : {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# ===============================================================
#  Main
# ===============================================================
if __name__ == "__main__":
    connection, channel_in = connect_rabbitmq()
    channel_out = connection.channel()
    channel_out.queue_declare(queue=QUEUE_OUT, durable=True)

    channel_in.basic_consume(
        queue=QUEUE_IN,
        on_message_callback=functools.partial(callback, channel_out=channel_out),
        auto_ack=False
    )

    print("[INFO] En attente de nouveaux fichiers CSV...")
    channel_in.start_consuming()