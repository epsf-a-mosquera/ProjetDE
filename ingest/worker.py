print("Ingest initialisé")
import json
import psycopg2
import pika

RABBITMQ_HOST = "rabbitmq"
QUEUE_NAME = "parsed.queue"

DB_HOST = "postgres"
DB_NAME = "scraping"
DB_USER = "scrape"
DB_PASS = "scrape"

def insert_into_db(data):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cursor = conn.cursor()
    query = """
        INSERT INTO items (title, summary, date, xml_file_path, status)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        data.get("title"),
        data.get("summary"),
        data.get("date"),
        data.get("xml_file_path"),
        "parsed"
    ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Données insérées : {data.get('xml_file_path')}")

def callback(ch, method, properties, body):
    data = json.loads(body)
    insert_into_db(data)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
    print("Ingest en attente de messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
print("Ingest terminé")