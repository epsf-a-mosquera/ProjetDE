#!/bin/bash
set -e

# ------------------------------
# Configuration
# ------------------------------

CSV_DIR="./data/csv_listes"
OLD_CSV_DIR="$CSV_DIR/old"
RABBIT_CONTAINER="projetde_rabbitmq"
POSTGRES_CONTAINER="projetde_postgres"
DB_NAME="DB_ERATV"
DB_USER="guest"
DB_PASS="guest"
DB_TABLE="liste_ERATV"

mkdir -p "$OLD_CSV_DIR"

# ------------------------------
# Fonctions utilitaires
# ------------------------------

# Attendre qu'une image Docker soit créée (boucle infinie)
wait_for_image() {
    local image_name=$1
    while ! docker image inspect "$image_name" >/dev/null 2>&1; do
        echo "[INFO] Attente création image $image_name..."
        sleep 2
    done
    echo "[INFO] Image $image_name créée."
}

# Attendre qu'un service Docker soit prêt (boucle infinie)
wait_for_container() {
    local container_name=$1
    local check_cmd=$2
    while ! docker exec "$container_name" sh -c "$check_cmd" >/dev/null 2>&1; do
        echo "[INFO] Service $container_name non prêt, attente 5s..."
        sleep 5
    done
    echo "[INFO] Service $container_name prêt."
}

# ------------------------------
# Étape 0 : Nettoyage
# ------------------------------

echo "=============================="
echo "Étape 0 : Nettoyage ancien CSV et Docker"
echo "=============================="

docker stop $(docker ps -q) 2>/dev/null || true
docker rm $(docker ps -a -q) 2>/dev/null || true
docker rmi $(docker images -q) 2>/dev/null || true
mv $CSV_DIR/*.csv $OLD_CSV_DIR/ 2>/dev/null || true

# ------------------------------
# Étape 1 : Lancer PostgreSQL, RabbitMQ et pgAdmin
# ------------------------------

echo "=============================="
echo "Étape 1 : Lancer PostgreSQL, RabbitMQ et pgAdmin"
echo "=============================="

# Construire les images
SERVICES=("postgres" "rabbitmq" "pgadmin")
docker-compose build "${SERVICES[@]}"

# Attendre que les images soient créées
for service in "${SERVICES[@]}"; do
    IMAGE_NAME=$(docker-compose config | grep "image: " | grep "$service" | awk '{print $2}')
    wait_for_image "$IMAGE_NAME"
done

# Lancer les containers
docker-compose up -d "${SERVICES[@]}"

# Vérifier services
wait_for_container "$RABBIT_CONTAINER" "rabbitmqctl status"
wait_for_container "$POSTGRES_CONTAINER" "pg_isready -U $DB_USER"

# Purge queues RabbitMQ si elles existent
for queue in csv_list.queue vehicle_pages.queue; do
    if docker exec $RABBIT_CONTAINER rabbitmqctl list_queues | grep -q "^$queue"; then
        docker exec $RABBIT_CONTAINER rabbitmqctl purge_queue $queue
        echo "[INFO] Queue '$queue' purgée."
    else
        echo "[INFO] Queue '$queue' non trouvée, skipping purge."
    fi
done

# ------------------------------
# Étape 2 : Lancer scraper_liste_html
# ------------------------------

echo "=============================="
echo "Étape 2 : Lancer scraper_liste_html"
echo "=============================="

docker-compose build scraper_liste_html
wait_for_image "$(docker-compose config | grep "image: " | grep scraper_liste_html | awk '{print $2}')"
docker-compose up -d scraper_liste_html

# Attente CSV (boucle infinie)
echo "[INFO] Attente génération CSV..."
while ! NEW_CSV=$(ls $CSV_DIR/*.csv 2>/dev/null | head -n 1); do
    sleep 5
done
echo "[INFO] CSV généré : $NEW_CSV"

echo "[INFO] Queues RabbitMQ actuelles :"
docker exec $RABBIT_CONTAINER rabbitmqctl list_queues

# ------------------------------
# Étape 3 : Lancer parser_liste_csv
# ------------------------------

echo "=============================="
echo "Étape 3 : Lancer parser_liste_csv"
echo "=============================="

docker-compose build parser_liste_csv
wait_for_image "$(docker-compose config | grep "image: " | grep parser_liste_csv | awk '{print $2}')"
docker-compose up -d parser_liste_csv

echo "[INFO] Suivi des logs parser_liste_csv en arrière-plan..."
docker logs -f projetde_parser_liste_csv &

# Attente traitement CSV (boucle infinie)
echo "[INFO] Attente que la table $DB_TABLE soit créée..."
while ! docker exec $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -c "\dt" | grep -q "$DB_TABLE"; do
    sleep 5
done
echo "[INFO] Parser terminé et table $DB_TABLE disponible."

# ------------------------------
# Étape 4 : Vérification des résultats
# ------------------------------

echo "=============================="
echo "Étape 4 : Vérification des résultats"
echo "=============================="

echo "[INFO] Queues RabbitMQ :"
docker exec $RABBIT_CONTAINER rabbitmqctl list_queues

echo "[INFO] Quelques lignes de la table $DB_TABLE :"
docker exec $POSTGRES_CONTAINER psql -U $DB_USER -d $DB_NAME -c "SELECT * FROM $DB_TABLE LIMIT 10;"

echo "=============================="
echo "Test terminé !"
echo "CSV généré : $NEW_CSV"
echo "Vérifier les logs parser pour voir les URLs publiées."
