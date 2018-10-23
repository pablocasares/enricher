---
title: Enrichment Query Language
layout: single
toc: true
---


*Enrichment Query Language* (EQL) is a query language written in ANTLR that allows us define that streams and fields to use and join. Before we look into details of the EQL, let's take a look at a few definitions of terms.

|Term|Definition|
|----------|----------|
|Stream|A stream represents continuosly updating data set of unknown size. A stream is an ordered, replayable, and fault-tolerant sequence of immutable data records|
|Data record|A data record is defined as key-value pair|
|Table|A table is a collection of key-value pairs|
|Joiner|A joiner merges two streams or tables based on the keys of their data records, and yields a new stream|
|Enricher| An enricher add relational information about events |


## Queries

Following provides an abstract diagram definition for EQL.

![](../assets/images/eql_syntax.png?raw=true)

### Joiners

A joiner merges two streams or tables based on the keys of their data records, and yields a new stream.

Joiner basic syntax is as follows:

`JOIN SELECT <comma-separated-fields or *> FROM (TABLE|STREAM) <stream-name> [BY <field-name>] USING <joiner-name>`

Where:

- `<comma-sparated-fields or *>` or `<field-name>` : The selected fields to join. The wildcard `*` indicates 'all fields of stream'.
- `<stream-name>` : The selected stream from which enricher will get data.
- `<joiner-name>` : The selected joiner in enrichment config json.

#### Optional BY

The `BY` clause allows us partition by field instead of stream's key. However this clause creates a new internal topic and need to write/read all messages.

We are going to illustrate this behaviour in next diagram:

![](../assets/images/joiner_partition_by.png?raw=true)

### Enrichers

An enricher add relational information about events.

Enricher basic syntax is as follows:

`ENRICH WITH <enricher-name>`

Where:
- `<enricher-name>` : The selected enricher in enrichment configuration json.

An enricher can be any data source that works with Json messages.

## Examples

### Simple field extraction

Suppose that we have two streams with fields:

- **inputStreamA**: fieldA and fieldB
- **inputStreamX**: fieldX and fieldY

If we define next query:
```sql
SELECT fieldA, fieldB, fieldY FROM inputStreamA, inputStreamB INSERT INTO STREAM outputStream
```

Enricher extracts fieldA, fieldB and fieldY from both inputStreamA and inputStreamB if exists and inserts them in outputStream.

This query is very simple and not enrich, Enricher only extracts and inserts fields.
![](../assets/images/simple_extract.png?raw=true)

### Simple streams join

If we define next query

```sql
SELECT * FROM STREAM inputStreamA JOIN SELECT fieldY FROM STREAM inputStreamX USING simpleStreamPreferredJoiner INSERT INTO STREAM outputStream
```

Enricher extracts all fields from intputStreamA and join them with the fieldY from inputStreamX using simpleStreamPreferredJoiner in order to do it is necessary that the streams share the same key.

![](../assets/images/simple_join.png?raw=true)

### Simple streams enrich

If we define next query
```sql
SELECT * FROM STREAM inputStreamA ENRICH WITH simpleStreamEnrich INSERT INTO STREAM outputSTREAM
```
Enricher extracts all fields from inputStreamA and enrich with simpleStreamEnrich using as data source a relational database.

![](../assets/images/simple_enrich.png?raw=true)

### Complex streams enrich and join

Suppose that we have a system with data about **flow** and **location** and we have a key-value store with information about ip **reputation**:

![](../assets/images/complex_join_and_enrich.png?raw=true)
We need enrich and join this information and send data to final stream **enrichflow**. In order to do this we have to define next EQL:

```sql
SELECT src, protocol FROM STREAM flow JOIN SELECT * FROM STREAM location USING simpleStreamPreferredJoiner ENRICH WITH reputationStreamEnrich INSERT INTO STREAM enrichflow
```

These are some examples about EQL and how Enricher works.
