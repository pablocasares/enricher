---
title: Base Tutorial
layout: single
toc: true
---

On this page, we can to try an enrichment stream example using a real Kafka cluster and the enricher jar artifact. We are going to suppose that you have built the enricher distribution how we explain on the [Building](https://github.com/wizzie-io/enricher#compiling-sources) section or you are using [enricher docker](https://hub.docker.com/r/wizzieio/enricher/).

### Explication
First of all, we need define an enrichment stream for launch an enrich application.

#### Enrichment Stream Config Json (my-enrichment-stream-tutorial.json)

```json
{
  "joiners":[
    {"name":"joinerStream", "className":"io.wizzie.enricher.enrichment.join.impl.StreamPreferredJoiner"}
  ],
  "queries": {
    "myquery": "SELECT timestamp, level FROM STREAM alarms JOIN SELECT * FROM STREAM metrics USING joinerStream INSERT INTO STREAM output"
  }
}
```

Once that we have defined our enrichment stream we inject messages

#### Phase 0: Input messages

We are going to suppose next (K,{V}) messages:

* topic: `metrics`
```
(   b64042f926eb,
    {
        "type": "cpu",
        "value": 90
    }
)
```

* topic: `alarms`
```
(
    b64042f926eb,
    {
        "timestamp": 1487869303,
        "type": "alarm",
        "level": "several"    
    }
)  
```

    As you can see we have two key-value messages with key `b64042f926eb`. On this example we read from both kafka topics `metrics` and `alarms`, join the streams with same key and then publish them on `output` kafka topic. In order to do this we have selected necessary fields and enricher joins them using a joiner function that is called `joinerStream`.

#### Phase 1: Output message
```json
{
  "timestamp": 1487869303,
  "name": "cpu",
  "value": 90,
  "level": "several"
}
```

Finally the result will be send to Kafka again into a topic that is called `output`.

### Execution
On the first time we need to have a running Kafka cluster and the decompressed enricher distribution.

#### Config file
We need to modify the config file that is inside the folder `config/sample_config.json`, we can change it or destroy it and create a new one with this content.

```json
{
  "application.id": "my-first-enricher-app",
  "bootstrap.servers": "localhost:9092",
  "num.stream.threads": 1,
  "bootstraper.classname": "io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper",
  "file.bootstraper.path": "/etc/enricher/my-enrichment-tutorial.json",
  "metric.enable": true,
  "metric.listeners": ["io.wizzie.metrics.ConsoleMetricListener"],
  "metric.interval": 60000
}
```
On this config file we indicate the `application.id` that will identify our instances group and the running Zookeeper and some Kafka Broker. On the example we are going to use the `FileBootstraper` so we read the config using a local file. We also need to set the property `file.bootstraper.path` to the path where we have the enrichment config file.

Now we can start the enricher service to do that we can uses the init script that is inside the folder bin:

```
enricher/bin/enricher-start.sh enricher/config/sample_config.json
```

When the enricher is running you can check it on the log file that is on directory `/var/log/ks-enricher/enricher.log` by default. If it started correctly you can see something like this:

```
2017-03-02 10:06:25         Builder [INFO] Execution plan:
2017-03-02 10:06:25         Builder [INFO] -------- TOPOLOGY BUILD START --------
2017-03-02 10:06:26     AdminUtils$ [INFO] Topic creation {"version":1,"partitions":{"2":[0],"1":[0],"3":[0],"0":[0]}}
2017-03-02 10:06:26   StreamBuilder [WARN] Join beetween stream isn't supported yet! The join is changed to use stream-table join
2017-03-02 10:06:26     AdminUtils$ [INFO] Topic creation {"version":1,"partitions":{"2":[0],"1":[0],"3":[0],"0":[0]}}
2017-03-02 10:06:26     AdminUtils$ [INFO] Topic creation {"version":1,"partitions":{"2":[0],"1":[0],"3":[0],"0":[0]}}
2017-03-02 10:06:26         Builder [INFO] --------  TOPOLOGY BUILD END  --------
```
The "*Topic creation*" messages indicates that one or more Kafka topics does not exist and has been created.

Now you can produce some input message first into `metrics` Kafka topic and then into `alarms`, but first you could open a Kafka consumer to check the output messages.

* Consumer from `output`
```
kafka_dist/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --property print.key=true --topic output --new-consumer
```

* Producer to `metrics`
```
kafka_dist/bin/kafka-console-producer.sh --broker-list localhost:9092 --property parse.key=true --property key.separator=, --topic metrics
```

You can write next message into console-producer:

```
b64042f926eb, {"name": "cpu", "value": 90}
```

* Producer to `alarms`
```
kafka_dist/bin/kafka-console-producer.sh --broker-list localhost:9092 --property parse.key=true --property key.separator=, --topic alarms
```

You can write next message into console-producer:

```
b64042f926eb, {"timestamp": 1487869303, "type": "alarm", "level": "several"}
```


and you must see the output message on the console-consumer:

```
b64042f926eb	{"name":"cpu","value":90,"level":"several","timestamp":1487869303}
```
