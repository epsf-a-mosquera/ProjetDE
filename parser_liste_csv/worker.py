print("Parser initialisé")
import os
import requests
import xml.etree.ElementTree as ET
import pika
import json

RABBITMQ_HOST = "rabbitmq"
INPUT_QUEUE = "xml_links.queue"
OUTPUT_QUEUE = "parsed.queue"
XML_DIR = "data/xml"

os.makedirs(XML_DIR, exist_ok=True)

def publish_parsed_data(data):
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=OUTPUT_QUEUE)
    channel.basic_publish(exchange='', routing_key=OUTPUT_QUEUE, body=json.dumps(data))
    connection.close()

def process_xml_link(link):
    response = requests.get(link)
    element_id = link.split("/")[-1].replace(".xml","")
    filename = os.path.join(XML_DIR, f"{element_id}.xml")
    
    # Sauvegarde du XML
    with open(filename, "wb") as f:
        f.write(response.content)

    # Lecture et parsing
    tree = ET.parse(filename)
    root = tree.getroot()
    data = {
        "title": root.findtext("title"),
        "summary": root.findtext("summary"),
        "date": root.findtext("date"),
        "xml_file_path": filename
    }
    publish_parsed_data(data)
    print(f"Parse et publié : {element_id}")

def callback(ch, method, properties, body):
    link = body.decode()
    process_xml_link(link)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=INPUT_QUEUE)
    channel.basic_consume(queue=INPUT_QUEUE, on_message_callback=callback, auto_ack=True)
    print("Parser XML en attente de messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
print("Parser terminé")