FROM confluentinc/cp-kafka-connect:7.6.1

USER root

COPY sink-elasticsearch-votes.json /opt/connect/sink-elasticsearch-votes.json

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:14.1.4

USER appuser
