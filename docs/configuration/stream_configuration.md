---
title: Stream Configuration
layout: single
toc: true
---

The stream configuration is the execution plan of the enricher. Enricher uses the stream configuration to build the Kafka Streams topology using DSL API. You can configure how the enricher gets the stream conf. To do it you need to use [Bootstrappers](http://wizzie-io.github.io/enricher/configuration/bootstrapper.html).

Stream configuration has three main sections: enrichers, joiners and queries

```json
{
  "enrichers": [
    {"name":"myEnricher", "className":"io.wizzie.enricher.enrichment.simple.MyCustomEnricher", "properties":{"property1":"value1", "property2":"value2"}}
  ],
  "joiners": [
    {"name":"streamPreferredJoiner", "className":"io.wizzie.enricher.enrichment.join.impl.StreamPreferredJoiner"}
  ],
  "queries": {
    "query1": "SELECT a,c,k,j FROM STREAM stream1, stream2 JOIN SELECT v FROM TABLE table1 USING streamPreferred INSERT INTO TABLE output"
  }
}
```

## Enrichers

The enrichers are simple message transformation process using external data, for example we can develop our own enricher to query a local database or load a file into memory and later use this data to enrich our messages.

The enrichers have three properties:
* `name`: The enricher's name.
* `className`: The enricher class reference.
* `properties`: The properties are a JSON Object that is deserialized into Java HashMap.

## Joiners

The joiners are used to merge information from two different Kafka topics. On the joiner class you can define how you can merge the data from different topics.

The enrichers have three properties:
* `name`: The enricher's name.
* `className`: The enricher class reference.

Currently, the enricher has three joiner types: [Base Joiner](http://wizzie-io.github.io/enricher/joiners/base-joiners.html), [Queryable Joiner](http://wizzie-io.github.io/enricher/joiners/queryable-joiners.html), [QueryableBack Joiner](http://wizzie-io.github.io/enricher/joiners/queryableback-joiners.html)

## Queries

Finally, the queries are the enrichment logic that you can define using the [SQL Syntax](http://wizzie-io.github.io/enricher/conf/sql-syntax.html). You can define multiple queries, inside the queries you can use the enricher and joiner definitions.

The `queries` property is a JSON Object where the `key` is the query's name and the `value` is the query.
