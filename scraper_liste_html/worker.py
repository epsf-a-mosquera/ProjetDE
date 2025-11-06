print("Scraper initialisé")

import requests
from bs4 import BeautifulSoup
import pika
import os

RABBITMQ_HOST = "rabbitmq"
QUEUE_NAME = "xml_links.queue"

def publish_link(link):
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=link)
    connection.close()

def main():
    url = "http://example.com/page"  # page contenant les liens XML
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extraction des liens XML
    links_xml = [a["href"] for a in soup.select("a.xml-link")]

    for link in links_xml:
        publish_link(link)
        print(f"Publié dans RabbitMQ : {link}")

if __name__ == "__main__":
    main()
print("Scraper terminé")