#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parser_liste_csv - worker RabbitMQ
----------------------------------
Consomme les messages de `csv_list.queue` envoyés par le scraper_liste_html,
lit le CSV correspondant (volume partagé), compare avec la base PostgreSQL,
envoie les nouvelles URLs dans `vehicle_pages.queue` et met à jour la DB.
"""

import os
import time
import socket
import pandas as pd
import psycopg2
import pika

# --------------------------------------
# Configurations
# --------------------------------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_IN = os.getenv("QUEUE_IN", "csv_list.queue")
QUEUE_OUT = os.getenv("QUEUE_OUT", "vehicle_pages.queue")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "DB_type_vehicule")
DB_USER = os.getenv("DB_USER", "guest")
DB_PASS = os.getenv("DB_PASS", "guest")

DATA_DIR = os.getenv("DATA_DIR", "/app/data/csv_listes")

MAX_RETRIES = 10
RETRY_DELAY = 5


# --------------------------------------
# Connexion PostgreSQL
# --------------------------------------
def connect_db():
    conn = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            print(f"[INFO] Connexion PostgreSQL réussie (tentative {attempt})")
            return conn
        except Exception as e:
            print(f"[WARN] PostgreSQL non prêt, retry {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY)
    raise Exception("Impossible de se connecter à PostgreSQL")


# --------------------------------------
# Connexion RabbitMQ
# --------------------------------------
def connect_rabbitmq():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_IN, durable=True)
            channel.queue_declare(queue=QUEUE_OUT, durable=True)
            print(f"[INFO] Connexion RabbitMQ réussie (tentative {attempt})")
            return connection, channel
        except (pika.exceptions.AMQPConnectionError, socket.gaierror) as e:
            print(f"[WARN] RabbitMQ non disponible, retry {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY)
    raise Exception("Impossible de se connecter à RabbitMQ")


# --------------------------------------
# Logique principale de parsing
# --------------------------------------
def process_csv(csv_path, cursor, conn, channel):
    print(f"[INFO] Traitement du fichier CSV : {csv_path}")
    if not os.path.exists(csv_path):
        print(f"[ERREUR] Fichier introuvable : {csv_path}")
        return

    csv_df = pd.read_csv(csv_path)
    cursor.execute("SELECT TypeID, EIN, TypeName, Status, LastUpdate, URL FROM vehicles")
    db_rows = cursor.fetchall()
    db_df = pd.DataFrame(db_rows, columns=["TypeID", "EIN", "TypeName", "Status", "LastUpdate", "URL"])

    db_dict = db_df.set_index("TypeID").to_dict("index")
    new_entries = []
    updated_entries = []

    for _, row in csv_df.iterrows():
        type_id = row["TypeID"]
        if type_id not in db_dict:
            new_entries.append(row)
        else:
            db_row = db_dict[type_id]
            if any(row[col] != db_row[col] for col in ["EIN", "TypeName", "Status", "LastUpdate", "URL"]):
                updated_entries.append(row)

    # Envoi des nouvelles URLs
    for row in new_entries:
        message = f"{row['TypeID']}|{row['URL']}"
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_OUT,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[INFO] Nouvelle URL envoyée : {message}")

    # Mise à jour DB
    for row in new_entries:
        cursor.execute(
            """
            INSERT INTO vehicles (TypeID, EIN, TypeName, Status, LastUpdate, URL)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (row["TypeID"], row["EIN"], row["TypeName"], row["Status"], row["LastUpdate"], row["URL"])
        )
    for row in updated_entries:
        cursor.execute(
            """
            UPDATE vehicles
            SET EIN=%s, TypeName=%s, Status=%s, LastUpdate=%s, URL=%s
            WHERE TypeID=%s
            """,
            (row["EIN"], row["TypeName"], row["Status"], row["LastUpdate"], row["URL"], row["TypeID"])
        )
    conn.commit()
    print(f"[INFO] DB mise à jour : {len(new_entries)} nouveaux, {len(updated_entries)} modifiés")


# --------------------------------------
# Callback RabbitMQ
# --------------------------------------
def on_message(ch, method, properties, body):
    message = body.decode()
    print(f"[INFO] Message reçu : {message}")
    if message.startswith("CSV ready:"):
        csv_path = message.replace("CSV ready:", "").strip()
        conn = connect_db()
        cursor = conn.cursor()
        process_csv(csv_path, cursor, conn, ch)
        cursor.close()
        conn.close()
    else:
        print(f"[WARN] Message inattendu : {message}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


# --------------------------------------
# Boucle principale
# --------------------------------------
def main():
    connection, channel = connect_rabbitmq()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_IN, on_message_callback=on_message)
    print(f"[INFO] En attente de messages sur {QUEUE_IN} ...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
