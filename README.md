
<p align="center">
    <img src="docs/assets/images/logo.svg" alt="Enricher" title="Enricher" width="250%"/>
</p>

[![CircleCI](https://circleci.com/gh/wizzie-io/enricher/tree/master.svg?style=shield)](https://circleci.com/gh/wizzie-io/enricher/tree/master)
[![Docs](https://img.shields.io/badge/docs-current-brightgreen.svg)](https://wizzie-io.github.io/enricher/)
[![GitHub release](https://img.shields.io/github/release/wizzie-io/enricher.svg)](https://github.com/wizzie-io/enricher/releases/latest)
[![wizzie-io](https://img.shields.io/badge/powered%20by-wizzie.io-F68D2E.svg)](https://github.com/wizzie-io/)

Enricher is a stream enriching engine based of Kafka Streams. Enricher does joins (streams and tables) end enrichment with any data store system that you define. You only need to define a JSON stream where you specify the joiners and enrichers to use and your enrichment query.

Enricher offers us: scalability, fault tolerance, back-preassure and full Kafka integration ... [Try it now!!](https://wizzie-io.github.io/enricher/getting/base-tutorial.html)

## Documentation

You can find the docs on the [Docs Website](https://wizzie-io.github.io/enricher/)

## Getting Started

You can get started with Enricher with this [tutorial](https://wizzie-io.github.io/enricher/getting/base-tutorial.html).

## Compiling Sources

To build this project you can use `maven` tool. 

If you want to build the JAR of the project you can do:

```
mvn clean package
```

If you want to check if it passes all the test:

```
mvn test
```

If you want to build the distribution tar.gz:

```
mvn clean package -P dist
```

If you want to build the docker image, make sure that you have the docker service running:

```
mvn clean package -P docker
```

## Contributing

1. [Fork it](https://github.com/wizzie-io/enricher/fork)
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Create a new Pull Request
