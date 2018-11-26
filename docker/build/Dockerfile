FROM java:openjdk-8-jre

MAINTAINER devel@wizzie.io

RUN apt-get update
RUN apt-get install -y gettext-base

RUN mkdir -p /var/log/enricher

ADD enricher-dist.tar.gz /opt/
RUN rm -f /opt/enricher/config/*

ADD log4j2_env.xml /opt/enricher/config/
ADD config_env.json /opt/enricher/config/
ADD enricher-start.sh /bin
ADD *.mmdb /opt/enricher/data/
ADD mac_vendors /opt/enricher/data/

ENV METRIC_ENABLE true
ENV METRIC_INTERVAL 60000
ENV MULTI_ID false
ENV NUM_STREAM_THREADS 1
ENV MAX_OUTPUT_KAFKA_TOPICS 500
ENV METRIC_KAFKA_TOPIC __metrics
ENV GLOBAL_TOPICS []
ENV BYPASS_NULL_KEYS true
ENV BOOTSTRAP_CLASSNAME io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper
ENV BOOTSTRAP_KAFKA_TOPICS [\"__enricher_bootstrap\"]

ENV LOG_LEVEL info
ENV GENERAL_LOG_LEVEL info

ENV JVM_OPTIONS -Xmx512m -Xms512m

CMD exec /bin/enricher-start.sh


