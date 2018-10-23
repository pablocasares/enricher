---
title: Metrics Configuration
layout: single
toc: true
---

The Enricher service uses the [Wizzie Metrics Library](https://github.com/wizzie-io/metrics-library) to build his metrics, by default it sends JVM metrics but you can register new metrics that will be exported too.

## Properties

The metrics service has three properties to configure it:

| Property   |      Description      |  Default Value |
|----------|---------------|-------|
| `metric.enable` |  Enable or disable metrics service | false|
| `metric.listeners` | The listener to send the metrics. |   ["io.wizzie.metrics.listeners.ConsoleMetricListener"] |
| `metric.interval` | The interval time to report metrics (milliseconds) |  60000  |
| `metric.verbose.mode`| Enable the verbose metric mode | false |

The listeners are the process that listen the reported metrics and do something with them. You can have multiple listeners at the same time. Currently, the enricher service supports two listener metrics:

## Listeners

### ConsoleMetricListener

This listener `io.wizzie.metrics.listeners.ConsoleMetricListener` send the transform the metrics to JSON and prints them into the log file using the log4j. The metric format is:

```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345}
```

### KafkaMetricListener

This listener `io.wizzie.metrics.listeners.KafkaMetricListener` send the transform the metrics to JSON and sends them into the Kafka topic. The metric format is:

```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345, "app_id":"MY_KAFKA_STREAMS_APP_ID"}
```

This listener adds a new property to specify the metrics Kafka topic `metric.kafka.topic`, by default is `__enricher_metrics`

### Custom Listeners
You can made new listeners to do this you need to implement the [MetricListener Class](https://github.com/wizzie-io/metrics-library/blob/master/src/main/java/io/wizzie/metrics/listeners/MetricListener.java).
