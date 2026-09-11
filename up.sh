#!/usr/bin/env bash

set -e
NETZWERK="kafka-lab"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/kafka-lab"

log() {
    echo ""
    echo ">>> $1"
}

fehler() {
    echo "Error: $1"
}

create_network() {
    # "docker network inspect" liefert Exit-Code 0, wenn es existiert
    # output würden nulliert
    if docker network inspect "$NETZWERK" > /dev/null 2>&1; then
        echo "Netzwerk $NETZWERK existiert schon"
    else
        docker network create "$NETZWERK"
        echo "Netzwerk $NETZWERK angelegt"
    fi
}

create_volume() {
    NAME="$1"
    if docker volume inspect "$NAME" > /dev/null 2>&1; then
        echo "Volume $NAME existiert schon"
    else
        docker volume create "$NAME"
        echo "Volume $NAME angelegt"
    fi
}


wait_until_healthy() {
    NAME="$1"
    MAX_SECONDS="$2"
    WAIT=0

    log "Warte auf $NAME (maximal $MAX_SECONDS Sekunden)"

    while true; do
        STATUS=$(docker inspect --format '{{.State.Status}}' "$NAME")

        # HEALTH: starting, healthy, unhealthy
        HEALTH=$(docker inspect --format '{{.State.Health.Status}}' "$NAME")

        if [ "$HEALTH" = "healthy" ]; then
            echo "$NAME ist bereit (nach $WAIT Sekunden)"
            return 0
        fi

        # anhalten, wenn weiterlauft nicht
        if [ "$STATUS" != "running" ]; then
            fehler "$NAME ist abgestürzt (Status: $STATUS)"
            echo "--- Letzte Logzeilen ---"
            docker logs "$NAME" --tail 30
            return 1
        fi

        if [ "$WAIT" -ge "$MAX_SECONDS" ]; then
            fehler "$NAME wurde nicht bereit (Status: $HEALTH)"
            docker logs "$NAME" --tail 30
            return 1
        fi

        sleep 2
        WAIT=$((WAIT + 2))
    done
}


start_container() {
    NAME="$1"
    # discard frist params and choose next
    shift

    # if it exists
    if docker inspect "$NAME" > /dev/null 2>&1; then

        STATUS=$(docker inspect --format '{{.State.Status}}' "$NAME")

        if [ "$STATUS" = "running" ]; then
            echo "$NAME läuft schon"
        else
            echo "$NAME war gestoppt, startet neu"
            docker start "$NAME"
        fi

    else
        echo "$NAME wird neu erstellt"
        # $@ forwards exactly remaining args
        docker run -d --name "$NAME" --network "$NETZWERK" "$@"
    fi
}


cd "$PROJECT_DIR"

log "Prüfe ob Docker läuft"
if ! docker info > /dev/null 2>&1; then
    fehler "Docker-Daemon läuft nicht. Erst Docker starten."
    exit 1
fi

log "Baue Images"
docker build -t lab/kafka    docker/kafka
docker build -t lab/postgres docker/postgres
docker build -t lab/kafka-ui docker/kafka-ui

log "Lege Netzwerk und Volumes an"
create_network
create_volume "kafka-data"
create_volume "pg-data"

log "Starte Kafka"
start_container "kafka" \
    -p 9092:9092 \
    -v kafka-data:/var/lib/kafka/data \
    lab/kafka

log "Starte Postgres"
# host 5433 -> container 5432 (5432 is often already taken locally); the app
# talks to postgres:5432 over the kafka-lab network, not this host port
start_container "postgres" \
    -p 5433:5432 \
    -v pg-data:/var/lib/postgresql \
    lab/postgres

wait_until_healthy "kafka" 90
wait_until_healthy "postgres" 60

log "Lege Kafka-Topics an"
docker exec kafka /opt/kafka/create-topics.sh

log "Starte Kafka-UI"
start_container "kafka-ui" -p 8080:8080 lab/kafka-ui

log "Fertig"
echo "Kafka-UI: http://localhost:8080"
echo ""
docker ps --filter "network=$NETZWERK"