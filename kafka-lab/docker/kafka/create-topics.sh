#!/usr/bin/env bash
set -euo pipefail
BS=localhost:19092
K=/opt/kafka/bin/kafka-topics.sh

create() {
  # independent thanks --if-not-exists i.e if exists create if not create
  $K --bootstrap-server "$BS" --create --if-not-exists --topic "$1" \
     --partitions "$2" --replication-factor 1 "${@:3}"
}

create quake.events.v1     3 --config retention.ms=6048000000
create quake.state.v1      3 --config cleanup.policy=compact
create quake.events.v1.DLT 1
create quake.alerts.v1     1 --config retention.ms=2592000000


$K --bootstrap-server "$BS" --list