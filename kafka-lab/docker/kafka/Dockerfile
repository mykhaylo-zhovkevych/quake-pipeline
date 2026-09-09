FROM apache/kafka:4.0.0
LABEL authors="mykhaylo zhovkevych"

# Onetime Cluster id
ENV CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
# node.id — unique ID of this node in the cluster
ENV KAFKA_NODE_ID=1
# process.roles — this process is both: data node AND metadata
ENV KAFKA_PROCESS_ROLES=broker,controller
# controller.quorum.voters — format: <id>@<host>:<port>
ENV KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
# listeners — what the broker ACTUALLY binds to (sockets)
ENV KAFKA_LISTENERS=INTERNAL://:19092,CONTROLLER://:9093,EXTERNAL://:9092
# advertised.listeners — what the broker ANSWERS to clients
ENV KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:19092,EXTERNAL://localhost:9092
# listener.security.protocol.map — INTERNAL/EXTERNAL
ENV KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
ENV KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
ENV KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
# log.dirs — where partition segments live. "log" means commit log
ENV KAFKA_LOG_DIRS=/var/lib/kafka/data


USER root
RUN mkdir -p /var/lib/kafka/data && chown -R 1000:1000 /var/lib/kafka/data
COPY --chown=1000:1000 --chmod=0755 create-topics.sh /opt/kafka/create-topics.sh
USER appuser

VOLUME ["/var/lib/kafka/data"]
EXPOSE 9092 19092

HEALTHCHECK --interval=10s --timeout=10s --retries=12 --start-period=20s \
  CMD /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server localhost:19092 >/dev/null 2>&1 || exit 1
