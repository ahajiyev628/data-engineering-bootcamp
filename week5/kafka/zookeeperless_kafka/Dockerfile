FROM openjdk:11.0.10-jre-buster

RUN apt-get update && \
    apt-get install -y curl python3 python3-pip git libpq-dev

ENV KAFKA_VERSION 3.8.0
ENV SCALA_VERSION 2.12

RUN  mkdir /tmp/kafka && \
    curl "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    -o /tmp/kafka/kafka.tgz && \
    mkdir /kafka && cd /kafka && \
    tar -xvzf /tmp/kafka/kafka.tgz --strip 1

RUN mkdir -p /data/kafka

COPY start-kafka.sh  /usr/bin

RUN chmod +x  /usr/bin/start-kafka.sh

RUN git clone https://github.com/erkansirin78/data-generator.git

# RUN pip3 install -r data-generator/requirements.txt

CMD ["start-kafka.sh"]