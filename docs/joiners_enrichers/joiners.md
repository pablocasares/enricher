---
title: Joiners
layout: single
toc: true
---

The joiners allows you join two streams yields results if both records in each stream are timely close to each other (i.e., both events happened within a certain time frame).

## Base Joiner

The base joiners join both two streams of JSON objects.

**Note:** If you are using JSON objects without common fields you can use any joiner.
{: .notice--info}

### StreamPreferredJoiner

The StreamPreferredJoiner is a joiner that allow us join both two streams of JSON objects, favoring the stream's fields.

<figure>
    <a href="{{ '/assets/images/stream_preferred_joiner.png' | relative_url }}"><img src="{{ '/assets/images/stream_preferred_joiner.png' | relative_url }}"></a>
</figure>

### TablePreferredJoiner

The TablePreferredJoiner is a joiner that allow us join both two streams of JSON objects, favoring the table's fields.

<figure>
    <a href="{{ '/assets/images/table_preferred_joiner.png' | relative_url }}"><img src="{{ '/assets/images/table_preferred_joiner.png' | relative_url }}"></a>
</figure>

## Queryable Joiner

The queryable joiner works like base joiner. The difference is that a queryable joiner will notify to `__enricher_queryable` topic, if received message cannot be join it with other message with same key. We can see two cases:

* CASE 1: Received message cannot be joined with other with same key.

<figure>
    <a href="{{ '/assets/images/queryable_joiner_case1.png' | relative_url }}">
    <img src="{{ '/assets/images/queryable_joiner_case1.png' | relative_url }}"></a>
</figure>

 1. Enricher receives a message from `stream` topic.
 2. As there is not any message from `table` topic, so enricher sends next notification to `__enricher_queryable`:
  ```json
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

<figure>
    <a href="{{ '/assets/images/queryable_joiner_case2.png' | relative_url }}"><img src="{{ '/assets/images/queryable_joiner_case2.png' | relative_url }}"></a>
</figure>

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

<figure>
    <a href="{{ '/assets/images/queryableback_preferred_concept.png' | relative_url }}"><img src="{{ '/assets/images/queryableback_preferred_concept.png' | relative_url }}"></a>
</figure>


Image above represents the queryableback join behaviour:

1. Enricher receives a message from `stream` topic.
2. As there is no any message from `table` topic, so enricher sends next notification to `__enricher_queryable` topic:
  ```json
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

**Caution** Steps 2 and 3 is a loop, if message isn't joined then they will be resend forever.
{: .notice--danger}

Just as base joiner, queryableback joiner have next functions:

|Function|Description|
|--------|-----------|
|StreamPreferredJoiner| Joiner that allows join both two streams of JSON objects, favoring the stream's fields.|
|TablePreferredJoiner| Joiner that allows join both two streams of JSON objects, favoring the tables's fields.|
