---
title: Enrichment Query Language
layout: single
toc: true
---

*Enrichment Query Language* (EQL) is a query language generated with [ANTLR](https://www.antlr.org/) that allows us define that streams and fields to use and join. Before we look into details of the EQL, let's take a look at a few definitions of terms.

|Term|Definition|
|----------|----------|
|Stream|A stream represents continuosly updating data set of unknown size. A stream is an ordered, replayable, and fault-tolerant sequence of immutable data records|
|Data record|A data record is defined as key-value pair|
|Table|A table is a collection of key-value pairs|
|Joiner|A joiner merges two streams or tables based on the keys of their data records, and yields a new stream|
|Enricher| An enricher add relational information about events |

## Queries

Following provides an abstract diagram definition for EQL.

<figure>
    <a href="{{ '/assets/images/eql_syntax.png' | relative_url }}"><img src="{{ '/assets/images/eql_syntax.png' | relative_url }}"></a>
</figure>

### Joiners

A joiner merges two streams or tables based on the keys of their data records, and yields a new stream.

Joiner basic syntax is as follows:

`JOIN SELECT <comma-separated-fields or *> FROM (TABLE|STREAM) <stream-name> [[PARTITION] BY <field-name>] USING <joiner-name>`

Where:

- `<comma-sparated-fields or *>` or `<field-name>` : The selected fields to join. The wildcard `*` indicates 'all fields of stream'.
- `<stream-name>` : The selected stream from which enricher will get data.
- `<joiner-name>` : The selected joiner in enrichment config json.

#### GLOBAL TABLE

The `FROM GLOBAL TABLE` clause allows us use a stream as [Kafka Stream GlobalKTable](https://docs.confluent.io/current/streams/concepts.html#globalktable).

#### Optional PARTITION BY

The `[PARTITION] BY` clause allows us partition by field instead of stream's key. We are going to illustrate this behaviour in next diagram:

<figure>
    <a href="{{ '/assets/images/joiner_partition_by.png' | relative_url }}"><img src="{{ '/assets/images/joiner_partition_by.png' | relative_url }}"></a>
</figure>

### Enrichers

An enricher adds relational information about events.

Enricher basic syntax is as follows:

`ENRICH WITH <enricher-name>`

Where:

- `<enricher-name>` : The selected enricher in enrichment configuration json.

An enricher can be any data source that works with Json messages.

### Output Stream

The `INSERT INTO` clause allows us to define which stream to use as the output stream, this clause can be complemented with `PARTITION BY` clause that allows us repartition the output stream by a specific field. If we omit the `PARTITION BY` clause, the enricher will use the current partition key of the input stream.

## Examples

### Simple field extraction

Suppose that we have two streams with fields:

- **inputStream1**: fieldA and fieldB
- **inputStream2**: fieldC and fieldD

If we define next query:
```sql
SELECT fieldA, fieldB, fieldC FROM inputStream1, inputStream2 INSERT INTO STREAM output
```

Enricher extracts fieldA, fieldB and fieldC from both inputStream1 and inputStream2 if exists and inserts them in output.

This query is very simple and not enrich, Enricher only extracts and inserts fields.

<figure>
    <a href="{{ '/assets/images/simple_extraction.png' | relative_url }}"><img src="{{ '/assets/images/simple_extraction.png' | relative_url }}"></a>
</figure>

### Simple streams join

If we define next query

```sql
SELECT * FROM STREAM inputStream1 JOIN SELECT fieldY FROM STREAM inputStream2 USING simpleStreamPreferredJoiner INSERT INTO STREAM output
```

Enricher extracts all fields from intputStream1 and join them with the fieldY from inputStream2 using simpleStreamPreferredJoiner in order to do it is necessary that the streams share the same key.

<figure>
    <a href="{{ '/assets/images/simple_join.png' | relative_url }}"><img src="{{ '/assets/images/simple_join.png' | relative_url }}"></a>
</figure>

### Simple streams enrich

If we define next query

```sql
SELECT * FROM STREAM input ENRICH WITH simpleStreamEnrich INSERT INTO STREAM output
```

Enricher extracts all fields from input and enrich with simpleStreamEnrich using as data source a relational database.

<figure>
    <a href="{{ '/assets/images/simple_enrich.png' | relative_url }}"><img src="{{ '/assets/images/simple_enrich.png' | relative_url }}"></a>
</figure>

### Complex streams enrich and join

Suppose that we have a system with data about **flow** and **location** and we have a key-value store with information about ip **reputation**:

<figure>
    <a href="{{ '/assets/images/complex_join_and_enrich.png' | relative_url }}"><img src="{{ '/assets/images/complex_join_and_enrich.png' | relative_url }}"></a>
</figure>

We need enrich and join this information and send data to final stream **enrichflow**. In order to do this we have to define next EQL:

```sql
SELECT src, protocol FROM STREAM flow JOIN SELECT * FROM STREAM location USING simpleStreamPreferredJoiner ENRICH WITH reputationStreamEnrich INSERT INTO STREAM enrichflow
```

These are some examples about EQL and how Enricher works.
