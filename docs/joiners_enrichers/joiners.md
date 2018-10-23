---
title: Joiners
layout: single
toc: true
---

## Base Joiner

The base joiners join both two streams of JSON objects.

*Note:* If you are using JSON objects without common fields you can use any joiner.

### StreamPreferredJoiner
The StreamPreferredJoiner is a joiner that allow us join both two streams of JSON objects, favoring the stream's fields.

Suppose next query:

`SELECT * FROM STREAM inputA JOIN SELECT * FROM TABLE inputB USING myStreamPreferredJoiner INSERT INTO STREAM output`

If we receive next message from `inputB` Kafka topic:
```json
{
 "FIELD-C": "VALUE-C",
 "FIELD-X": "VALUE-Y"
}
```

And then we receive next message from `inputA` Kafka topic:
```json
{
 "FIELD-A": "VALUE-A",
 "FIELD-B": "VALUE-B",
 "FIELD-X": "VALUE-X"
}
```
We will receive next message from `output` Kafka topic:
```json
{
 "FIELD-A": "VALUE-A",
 "FIELD-B": "VALUE-B",
 "FIELD-C": "VALUE-C",
 "FIELD-X": "VALUE-X"
}
```
### TablePreferredJoiner
The TablePreferredJoiner is a joiner that allow us join both two streams of JSON objects, favoring the table's fields.

Suppose next query:

`SELECT * FROM STREAM inputA JOIN SELECT * FROM TABLE inputB USING myTablePreferredJoiner INSERT INTO STREAM output`

If we receive next message from `inputB` Kafka topic:
```json
{
 "FIELD-C": "VALUE-C",
 "FIELD-X": "VALUE-Y"
}
```

And then we receive next message from `inputA` Kafka topic:
```json
{
 "FIELD-A": "VALUE-A",
 "FIELD-B": "VALUE-B",
 "FIELD-X": "VALUE-X"
}
```
We will receive next message from `output` Kafka topic:
```json
{
 "FIELD-A": "VALUE-A",
 "FIELD-B": "VALUE-B",
 "FIELD-C": "VALUE-C",
 "FIELD-X": "VALUE-Y"
}
```

## Queryable Joiner

The queryable joiner works like base joiner. The difference is that a queryable joiner will notify to `__enricher_queryable` topic, if received message cannot be join it with other message with same key.

![](../assets/images/queryable_preferred_concept.png?raw=true)

Image above represents the queryable joiner behaviour, we can see two cases:

* CASE 1: Received message cannot be joined with other with same key.

 1. Enricher receives a message from `stream` topic.
 2. As there is not any message from `table` topic, so enricher sends next notification to `__enricher_queryable`:
  ```
     (
       KEY-A,
       {
         "joiner":"joinerStream",
         "type":"joiner-query",
         "table":"metrics",
         "joiner-status":false
       }
     )
  ```
 3. The Unjoined message is sends to `output` topic.

* CASE 2: Received message can be joined with other with same key.

 1. A message is received in `table` topic.
 2. Enricher receives a message from `stream` topic with same key that message received in `table` topic.
 3. Enricher joins the messages with same key and sends it to `output` topic:

Just the base joiner, queryable joiner have next functions:

|Function|Description|
|--------|-----------|
|StreamPreferredJoiner| Joiner that allows join both two streams of JSON objects, favoring the stream's fields.|
|TablePreferredJoiner| Joiner that allows join both two streams of JSON objects, favoring the tables's fields.|

## Queryableback Joiner

The queryableback joiner work like base joiner. The difference is that a queryableback joiner will notify to `__enricher_queryable` and will resend message to stream, if received message cannot be join it with other message with same key.

![](../assets/images/queryableback_preferred_concept.png?raw=true)

Image above represents the queryableback join behaviour:

1. Enricher receives a message from `stream` topic.
2. As there is no any message from `table` topic, so enricher sends next notification to `__enricher_queryable` topic:

  ```
     (
       KEY-A,
       {
         "joiner":"joinerStream",
         "type":"joiner-query",
         "table":"metrics",
         "joiner-status":false
       }
     )
  ```
3. Enricher resends the message to `stream` topic again.
4. Enricher receives the message from `table` topic.
5. Enricher joins the messages and sends to `output` topic.

Just as base joiner, queryableback joiner have next functions:

|Function|Description|
|--------|-----------|
|StreamPreferredJoiner| Joiner that allows join both two streams of JSON objects, favoring the stream's fields.|
|TablePreferredJoiner| Joiner that allows join both two streams of JSON objects, favoring the tables's fields.|

**_Caution_** Steps 2 and 3 is a loop, if message isn't joined then they will be resend forever.
