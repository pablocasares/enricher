---
title: Docker Example
layout: single
toc: false
---

We are going to follow the [Base tutorial](https://wizzie-io.github.io/enricher/getting_started/base_tutorial) but using docker and docker-compose.

Make sure you have Docker installed properly before following any of the steps below. Visit the [official Docker site](https://www.docker.com) for further instructions on the installation of Docker.
{: .notice--info}

To do this example you need a kafka broker and a zookeeper server, you can use the next [docker componse](https://docs.docker.com/compose/) file to run both:

```docker
version: '3.6'

services:
  zookeeper:
    image: wurstmeister/zookeeper:3.4.6
    container_name: zk-svc
    ports:
      - "2181:2181"
    networks:
      - kafka

  kafka:
    image: wurstmeister/kafka:1.1.0
    container_name: kafka-svc
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_HOST_NAME: ${KAFKA_ADVERTISED_HOST_NAME}
      KAFKA_ZOOKEEPER_CONNECT: zk-svc:2181
    networks:
      - kafka

networks:
  kafka:
    driver: bridge
```

Where `KAFKA_ADVERTISED_HOST_NAME` is your host IP, you can also create a `.env` file with next content:

```bash
KAFKA_ADVERTISED_HOST_NAME=192.168.1.15
```

Now to run kafka and zookeeper you need to use the next command:

```bash
docker-compose up
```

You can use `-d` flag to run containers in the background and print new container names
{: .notice}

## Go to Enrich!

First we need to create the next topics:

- `alarms`:
This topic contains information about alarms of sensors.
- `metrics`:
This topic contains information about sensor's metrics.
- `output`:
This topic will contain the final result.

To create them run the next commands:

Create topic `metrics`
```bash
docker exec -it kafka-svc kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic metrics --zookeeper zk-svc
```

Create topic `alarms`
```bash
docker exec -it kafka-svc kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic alarms --zookeeper zk-svc
```

Create topic `output`
```bash
docker exec -it kafka-svc kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic output --zookeeper zk-svc
```

Now you need to run an instance of enricher, to do it run the next command:

```bash
docker run --rm --name enricher-svc -e APPLICATION_ID=my-enricher-app -e KAFKA_BOOTSTRAP_SERVER=192.168.1.15:9092 wizzieio/enricher
```

You can use `-d` flag to run container in background and print container ID
{: .notice}

Now you need to load a enrichment query in Enricher, we are going to use the next enrichment plan:

```json
{
  "joiners":[
    {"name":"streamPreferred", "className":"io.wizzie.enricher.enrichment.join.impl.queryable.StreamPreferredJoiner"}
  ],
  "queries": {
    "query1": "SELECT * FROM STREAM metrics JOIN SELECT level FROM TABLE alarms USING streamPreferred INSERT INTO TABLE output"
   }
}
```

In `queries` you can see the `query1`, this query is meaning the next:

Select all fields of stream `metrics` and join with the field `level` of table `alarms` using the `streamPreferred` joiner and put the result in the table `output`.
{: .notice--primary}

You can know about `streamPreferred` in [Joiners](https://wizzie-io.github.io/enricher/joiners_enrichers/joiners) section
{: .notice--info}

To do it you must create a producer to `__enricher_bootstrap` topic:

```bash
docker exec -it kafka-svc kafka-console-producer.sh --topic __enricher_bootstrap --broker-list 192.168.1.15:9092 --property "parse.key=true" --property "key.separator=:"
```

Now only copy & paste the next line to producer:

```text
my-enricher-app:{"joiners":[{"name":"streamPreferred","className":"io.wizzie.enricher.enrichment.join.impl.queryable.StreamPreferredJoiner"}],"queries":{"query1":"SELECT * FROM STREAM metrics JOIN SELECT level FROM TABLE alarms USING streamPreferred INSERT INTO TABLE output"}}
```

Now you need two kafka producers and one kafka consumer in different terminals, so you must to run the next commands:

Producer to topic `metrics`
```bash
docker exec -it kafka-svc kafka-console-producer.sh --topic metrics --broker-list 192.168.1.15:9092 --property "parse.key=true" --property "key.separator=:"
```

Producer to topic `alarms`
```bash
docker exec -it kafka-svc kafka-console-producer.sh --topic alarms --broker-list 192.168.1.15:9092 --property "parse.key=true" --property "key.separator=:"
```

Consumer from topic `output`
```bash
docker exec -it kafka-svc kafka-console-consumer.sh --topic output --bootstrap-server 192.168.1.15:9092 --property "print.key=true" --property "key.separator= -> "
```

Now we need to send the next messages:

Message to topic `alarms`
```bash
b64042f926eb:{"timestamp": 1487869303,"type": "alarm","level": "several"}
```

Message to topic `metrics`
```bash
b64042f926eb:{"type":"cpu","value":90}
```

So in the output topic we will get the next message:

```bash
b64042f926eb -> {"type":"cpu","level":"several","value":90}
```

The image below shows you how enricher works:

<figure>
    <a href="{{ '/assets/images/docker_example.png' | relative_url }}"><img src="{{ '/assets/images/docker_example.png' | relative_url }}"></a>
</figure>

 You have completed the docker example, congratulations!
