FROM openjdk:8-jre-slim

MAINTAINER devel@wizzie.io

RUN apt-get update -y && apt-get install -y gettext-base wget && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/log/enricher

ADD enricher-dist.tar.gz /opt/
RUN rm -f /opt/enricher/config/*

ADD log4j2_env.xml /opt/enricher/config/
ADD config_env.json /opt/enricher/config/
ADD enricher-start.sh /bin
ADD mac_vendors /opt/enricher/data/

RUN wget -q http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz && tar --transform='s|.*/||' -xvf GeoLite2-City.tar.gz -C /opt/enricher/data --wildcards GeoLite*/*.mmdb &&  wget -q http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN.tar.gz && tar --transform='s|.*/||' -xvf GeoLite2-ASN.tar.gz -C /opt/enricher/data --wildcards GeoLite*/*.mmdb && rm -rf *.tar.gz

ENV METRIC_ENABLE=true \
    METRIC_INTERVAL=60000 \
    MULTI_ID=false \
    NUM_STREAM_THREADS=1 \
    MAX_OUTPUT_KAFKA_TOPICS=500 \
    METRIC_KAFKA_TOPIC=__metrics \
    GLOBAL_TOPICS=[] \
    BYPASS_NULL_KEYS=true \
    BOOTSTRAP_CLASSNAME=io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper \
    BOOTSTRAP_KAFKA_TOPICS=[\"__enricher_bootstrap\"] \
    LOG_LEVEL=info \
    GENERAL_LOG_LEVEL=info \
    JVM_OPTIONS="-Xmx512m -Xms512m"

CMD exec /bin/enricher-start.sh


