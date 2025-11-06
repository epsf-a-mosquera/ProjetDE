#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
worker.py
---------
Parse le CSV généré par scraper_liste_html, compare avec la DB PostgreSQL,
envoie les nouvelles URLs à vehicle_pages.queue et met à jour la DB.
"""

import os
import time
import socket
import pandas as pd
import psycopg2
import pika
from datetime import datetime

# --------------------------------------
# Configuration
# --------------------------------------
DATA_DIR = os.getenv("DATA_DIR", "/app/data/csv_listes")
CSV_FILES = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".csv")])
if not CSV_FILES:
    raise FileNotFoundError(f"Aucun CSV trouvé dans {DATA_DIR}")
LATEST_CSV = os.path.join(DATA_DIR, CSV_FILES[-1])

# RabbitMQ
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_OUT = os.getenv("QUEUE_OUT", "vehicle_pages.queue")

# PostgreSQL
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "scraping")
DB_USER = os.getenv("DB_USER", "scrape")
DB_PASS = os.getenv("DB_PASS", "scrape")

# Retry parameters RabbitMQ
MAX_RETRIES = 10
RETRY_DELAY = 5

# --------------------------------------
# Connexion RabbitMQ
# --------------------------------------
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
        channel.queue_declare(queue=QUEUE_OUT, durable=True)
        print(f"[INFO] Connecté à RabbitMQ après {attempt} tentative(s)")
        break
    except (pika.exceptions.AMQPConnectionError, socket.gaierror) as e:
        print(f"[WARN] RabbitMQ non disponible, retry {attempt}/{MAX_RETRIES} ({e})")
        time.sleep(RETRY_DELAY)
else:
    raise Exception("Impossible de se connecter à RabbitMQ après plusieurs tentatives")

# --------------------------------------
# Connexion PostgreSQL
# --------------------------------------
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cursor = conn.cursor()

# --------------------------------------
# Fonctions principales
# --------------------------------------

def load_csv(file_path):
    """Charge le CSV et renvoie un DataFrame"""
    df = pd.read_csv(file_path)
    return df

def fetch_db_types():
    """Récupère les données existantes dans la DB"""
    cursor.execute("SELECT TypeID, EIN, TypeName, Status, LastUpdate, URL FROM vehicles")
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["TypeID", "EIN", "TypeName", "Status", "LastUpdate", "URL"])
    return df

def compare_and_update(csv_df, db_df):
    """Compare CSV vs DB et retourne nouveaux types et modifs"""
    new_entries = []
    updated_entries = []

    db_dict = db_df.set_index("TypeID").to_dict("index")

    for _, row in csv_df.iterrows():
        type_id = row["TypeID"]
        if type_id not in db_dict:
            new_entries.append(row)
        else:
            db_row = db_dict[type_id]
            # Comparer les champs (sauf TypeID)
            if any(row[field] != db_row[field] for field in ["EIN","TypeName","Status","LastUpdate","URL"]):
                updated_entries.append(row)

    return new_entries, updated_entries

def send_new_urls(new_entries):
    """Envoie les URLs des nouveaux types à RabbitMQ"""
    for row in new_entries:
        url = row["URL"]
        message = f"{row['TypeID']}|{url}"
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_OUT,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"[INFO] URL envoyée à queue: {message}")

def update_db(new_entries, updated_entries):
    """Insère et met à jour les données dans PostgreSQL"""
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
    print(f"[INFO] DB mise à jour: {len(new_entries)} nouveaux, {len(updated_entries)} modifiés")

# --------------------------------------
# Main
# --------------------------------------
def main():
    print(f"[INFO] Lecture du CSV: {LATEST_CSV}")
    csv_df = load_csv(LATEST_CSV)
    db_df = fetch_db_types()
    new_entries, updated_entries = compare_and_update(csv_df, db_df)

    if new_entries:
        send_new_urls(new_entries)
    if new_entries or updated_entries:
        update_db(new_entries, updated_entries)

    connection.close()
    cursor.close()
    conn.close()
    print("[INFO] Worker terminé.")

if __name__ == "__main__":
    main()
