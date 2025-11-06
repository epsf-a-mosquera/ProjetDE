# -*- coding: utf-8 -*-
"""
Scraper liste HTML - ERATV avec Selenium Remote

✅ Télécharge le XML complet dans le volume partagé
✅ Publie le chemin du fichier XML dans RabbitMQ
"""

import os
import time
import random
import pika
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# ---------------------- CONFIG ----------------------
BASE_URL = "https://eratv.era.europa.eu"
LIST_URL = BASE_URL + "/Eratv/Home/List"
TEMP_FILE = "ERATV - List of Authorisations.xml"

# Volume partagé avec Selenium
DATA_DIR = "/app/data/xml/listes"
os.makedirs(DATA_DIR, exist_ok=True)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.environ.get("QUEUE_NAME", "xml_list.queue")
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
log_path = os.path.join(LOG_DIR, f"scraper_liste_html_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# ---------------------- SELENIUM ----------------------
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

# Dossier de téléchargement dans Selenium (volume partagé)
prefs = {
    "download.default_directory": "/home/selenium/Downloads",
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

# Connexion au Selenium Remote WebDriver
driver = webdriver.Remote(
    command_executor='http://selenium:4444/wd/hub',
    options=chrome_options
)
wait = WebDriverWait(driver, 30)

# ---------------------- FONCTIONS ----------------------
def human_pause(min_sec=1, max_sec=3):
    pause_time = random.randint(min_sec, max_sec)
    print(f"⏳ Pause {pause_time} secondes...")
    time.sleep(pause_time)

def wait_for_download(download_dir, filename, timeout=180):
    end_time = time.time() + timeout
    temp_file = os.path.join(download_dir, filename)
    while True:
        files = os.listdir(download_dir)
        downloading = [f for f in files if f.endswith(".crdownload") or f.endswith(".part")]
        if os.path.exists(temp_file) and not downloading:
            return temp_file
        if time.time() > end_time:
            raise Exception("❌ XML non téléchargé dans le délai imparti")
        time.sleep(1)

# ---------------------- SCRAPING ----------------------
try:
    print("⬇️ Ouverture de la page List ERATV...")
    driver.get(LIST_URL)
    wait.until(EC.presence_of_element_located((By.ID, "SearchForm")))
    human_pause()

    # Cocher tous les pays
    checkboxes = driver.find_elements(By.CSS_SELECTOR, "div.GroupChecks.HorChecks input[type='checkbox']")
    print(f"⬇️ {len(checkboxes)} pays trouvés, on coche tout...")
    for checkbox in checkboxes:
        if not checkbox.is_selected():
            checkbox.click()
            time.sleep(0.05)
    human_pause()

    # Sélectionner "Export XML"
    export_select = driver.find_element(By.ID, "ExportList")
    for option in export_select.find_elements(By.TAG_NAME, "option"):
        if option.get_attribute("value") == "2":
            option.click()
            break
    human_pause()

    # Cliquer sur le bouton "Export"
    driver.find_element(By.ID, "btnExport").click()
    print("⬇️ Export XML demandé...")
    human_pause()

    # ---------------------- ATTENTE DU TÉLÉCHARGEMENT ----------------------
    temp_path = wait_for_download(DATA_DIR, TEMP_FILE)
    print(f"✅ Téléchargement terminé : {temp_path}")

    # Renommer avec timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_path = os.path.join(DATA_DIR, f"eratv_list_{timestamp}.xml")
    os.rename(temp_path, final_path)
    print(f"✅ Fichier XML sauvegardé : {final_path}")

    # ---------------------- PUBLIER DANS RABBITMQ ----------------------
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=final_path)
    print(f"📤 Lien XML publié dans RabbitMQ ({QUEUE_NAME})")

except Exception as e:
    print(f"❌ Erreur : {e}")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] ❌ Erreur : {e}\n")

finally:
    driver.quit()
    print("🛑 Selenium fermé")
    print("✅ Script scraper_liste_html terminé")
