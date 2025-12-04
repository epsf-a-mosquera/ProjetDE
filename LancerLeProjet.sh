Étape 0 : Nettoyage
# Stop tous les conteneurs Docker
docker stop $(docker ps -q) 2>/dev/null || true

# Supprime tous les conteneurs Docker
docker rm $(docker ps -a -q) 2>/dev/null || true

# Déplace les anciens CSV dans le dossier old
mv ./data/csv_listes/*.csv ./data/csv_listes/old/ 2>/dev/null || true

Étape 1 : Lancer PostgreSQL, RabbitMQ et pgAdmin
# Lancer les conteneurs définis dans docker-compose
docker compose up -d postgres rabbitmq pgadmin


Ensuite, il y a des commandes pour vérifier que PostgreSQL et RabbitMQ sont prêts :

# Vérifier que PostgreSQL est prêt
docker exec projetde_postgres pg_isready -U guest

# Vérifier que RabbitMQ est healthy
docker inspect --format='{{.State.Health.Status}}' projetde_rabbitmq

# Vérifier que l'application RabbitMQ est démarrée
docker exec projetde_rabbitmq rabbitmq-diagnostics check_running

# Si nécessaire, forcer le démarrage de RabbitMQ
docker exec projetde_rabbitmq rabbitmqctl start_app


Purge des queues si elles existent :

docker exec projetde_rabbitmq rabbitmqctl list_queues -q | grep -q "^csv_list.queue"
docker exec projetde_rabbitmq rabbitmqctl purge_queue csv_list.queue || true

docker exec projetde_rabbitmq rabbitmqctl list_queues -q | grep -q "^vehicle_pages.queue"
docker exec projetde_rabbitmq rabbitmqctl purge_queue vehicle_pages.queue || true

Étape 2 : Lancer les services custom

Pour chaque service de la liste :

# Construire et lancer scraper_liste_html
docker compose build scraper_liste_html
docker compose up -d scraper_liste_html

# Construire et lancer parser_liste_csv
docker compose build parser_liste_csv
docker compose up -d parser_liste_csv

# Construire et lancer scraper_type_vehicule_html
docker compose build scraper_type_vehicule_html
docker compose up -d scraper_type_vehicule_html

# Construire et lancer parser_type_vehicule_xml
docker compose build parser_type_vehicule_xml
docker compose up -d parser_type_vehicule_xml

# Construire et lancer ingest
docker compose build ingest
docker compose up -d ingest

# Construire et lancer scheduler
docker compose build scheduler
docker compose up -d scheduler

# Construire et lancer ml_trainer
docker compose build ml_trainer
docker compose up -d ml_trainer

# Construire et lancer ml_api
docker compose build ml_api
docker compose up -d ml_api

# Construire et lancer api
docker compose build api
docker compose up -d api

Étape 3 : Attente CSV et suivi parser
# Afficher les logs du parser
docker logs -f projetde_parser_liste_csv &

# Vérifier création de la table dans PostgreSQL
docker exec projetde_postgres psql -U guest -d DB_ERATV -c "\dt" | grep -q "liste_ERATV"

Étape 4 : Vérifications finales
# Liste des queues RabbitMQ
docker exec projetde_rabbitmq rabbitmqctl list_queues

# Afficher quelques lignes de la table dans PostgreSQL
docker exec projetde_postgres psql -U guest -d DB_ERATV -c "SELECT * FROM liste_ERATV LIMIT 10;"
