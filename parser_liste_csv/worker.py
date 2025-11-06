#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parser_liste_csv/worker.py
--------------------------
Lit le CSV généré par scraper_liste_html, compare avec la table PostgreSQL 'liste_ERATV',
détecte les nouveaux ou modifiés, met à jour la base, et envoie leurs URLs
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

CSV_DIR = "/app/data/csv/listes"

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
# Comparaison CSV vs DB
# ------------------------------------------------------------
def compare_and_update(csv_path, db_conn, rabbit_channel):
    print(f"[INFO] Lecture du CSV : {csv_path}")
    df_csv = pd.read_csv(csv_path, dtype=str).fillna("")

    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT Type_ID, Authorisation_document_reference_EIN, Type_Name,
                   Authorisation_Status, Last_Update, URL_TV
            FROM liste_ERATV
        """)
        rows = cur.fetchall()

    db_df = pd.DataFrame(rows, columns=[
        "Type_ID", "Authorisation_document_reference_EIN",
        "Type_Name", "Authorisation_Status", "Last_Update", "URL_TV"
    ])
    db_df = db_df.astype(str).fillna("")

    urls_to_publish = []

    with db_conn.cursor() as cur:
        for _, row in df_csv.iterrows():
            csv_rec = row.to_dict()
            type_id = csv_rec["TypeID"]

            # Adapter les noms de colonnes CSV aux colonnes SQL
            ein = csv_rec.get("EIN", "")
            name = csv_rec.get("TypeName", "")
            status = csv_rec.get("Status", "")
            last_update = csv_rec.get("LastUpdate", "")
            url_tv = csv_rec.get("URL", "")

            existing = db_df[db_df["Type_ID"] == type_id]

            if existing.empty:
                # Nouveau véhicule
                cur.execute("""
                    INSERT INTO liste_ERATV
                    (Type_ID, Authorisation_document_reference_EIN, Type_Name,
                     Authorisation_Status, Last_Update, URL_TV)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (type_id, ein, name, status, last_update, url_tv))
                urls_to_publish.append(url_tv)
                print(f"[NEW] {type_id} ajouté.")

            else:
                db_rec = existing.iloc[0].to_dict()
                changed = any([
                    ein != db_rec["Authorisation_document_reference_EIN"],
                    name != db_rec["Type_Name"],
                    status != db_rec["Authorisation_Status"],
                    last_update != db_rec["Last_Update"],
                    url_tv != db_rec["URL_TV"],
                ])

                if changed:
                    cur.execute("""
                        UPDATE liste_ERATV
                        SET Authorisation_document_reference_EIN=%s,
                            Type_Name=%s,
                            Authorisation_Status=%s,
                            Last_Update=%s,
                            URL_TV=%s,
                            updated_at=NOW()
                        WHERE Type_ID=%s
                    """, (ein, name, status, last_update, url_tv, type_id))
                    urls_to_publish.append(url_tv)
                    print(f"[UPDATED] {type_id} mis à jour.")

    db_conn.commit()
    print(f"[INFO] {len(urls_to_publish)} nouveaux ou modifiés détectés.")

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
