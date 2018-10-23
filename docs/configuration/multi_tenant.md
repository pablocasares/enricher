---
layout: single
title: Multi Tenant
toc: false
---

The Enricher service has a multi tenant mode, on this mode it prefix the `application.id` automatically on all the Kafka topics, except the bootstraper and metric topic.

On this mode when you define a stream definition, for example:

```json
{
  "joiners": [
    {"name":"streamPreferred", "className":"io.wizzie.enricher.enrichment.join.impl.StreamPreferredJoiner"}
  ],
  "queries": {
    "query1": "SELECT * FROM STREAM topic1 JOIN SELECT dim1,dim2 FROM TABLE topic2 USING streamPreferred INSERT INTO TABLE output"
  }
}
```

You define the topic `topic1` and `topic2` but actually you read from Kafka topic `${APP_ID}_topic1` and `${APP_ID}_topic2` and when you produce to the `output` topic, you really send data to the topic `${APP_ID}_output`. On this mode we define the `APP_ID == TENANT_ID`. To enable this mode you can configure the property `multi.id` to `true`.

## Global topics

For the topics defined at the property `global.topics` the application.id is not prefixed. This feature allows you the use of the topics without application.id prefix you defined at this property.
This may be used when different enricher instances read from a common topic and you don't want to have one topic to read the enrichment events for each one.
