package io.wizzie.enricher.builder;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.base.builder.config.ConfigProperties;
import io.wizzie.enricher.enrichment.join.BaseJoiner;
import io.wizzie.enricher.enrichment.join.Joiner;
import io.wizzie.enricher.enrichment.join.QueryableBackJoiner;
import io.wizzie.enricher.enrichment.join.QueryableJoiner;
import io.wizzie.enricher.enrichment.simple.BaseEnrich;
import io.wizzie.enricher.enrichment.simple.Enrich;
import io.wizzie.enricher.exceptions.EnricherNotFound;
import io.wizzie.enricher.exceptions.JoinerNotFound;
import io.wizzie.enricher.model.PlanModel;
import io.wizzie.enricher.model.exceptions.PlanBuilderException;
import io.wizzie.enricher.query.antlr4.Join;
import io.wizzie.enricher.query.antlr4.Select;
import io.wizzie.enricher.query.antlr4.Stream;
import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.wizzie.enricher.base.utils.Constants.__KEY;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

public class StreamBuilder {
    String appId;
    MetricsManager metricsManager;
    Config config;
    Map<String, KStream<String, Map<String, Object>>> streams;
    Map<String, KTable<String, Map<String, Object>>> tables;
    Map<String, GlobalKTable<String, Map<String, Object>>> globalTables;
    Map<String, Joiner> joiners = new HashMap<>();
    Map<String, Enrich> enrichers = new HashMap<>();
    List<String> globalTopics = new LinkedList<>();

    public StreamBuilder(Config config, MetricsManager metricsManager) {
        this.appId = config.get(APPLICATION_ID_CONFIG);
        this.config = config;
        this.metricsManager = metricsManager;
        this.streams = new HashMap<>();
        this.tables = new HashMap<>();
        this.globalTables = new HashMap<>();
        this.globalTopics = config.getOrDefault(ConfigProperties.GLOBAL_TOPICS, new LinkedList<String>());
    }

    private static final Logger log = LoggerFactory.getLogger(StreamBuilder.class);

    public StreamsBuilder builder(PlanModel model) throws PlanBuilderException {
        model.validate(config.clone());
        clean();

        StreamsBuilder builder = new StreamsBuilder();

        buildInstances(model);
        addStreams(model, builder);
        addTables(model, builder);
        addEnriches(model);
        addInserts(model);

        return builder;
    }

    private void buildInstances(PlanModel model) {
        model.getEnrichers().forEach(enrichModel -> {
            try {
                Enrich enrich = makeInstance(enrichModel.getClassName());
                enrich.init(enrichModel.getProperties(), metricsManager);
                enrichers.put(enrichModel.getName(), enrich);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the function {}", enrichModel.getClassName());
            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the function " + enrichModel.getClassName(), e);
            }
        });

        model.getJoiners().forEach(joinerModel -> {
            try {
                Joiner joiner = makeInstance(joinerModel.getClassName());
                joiner.init(joinerModel.getName());
                joiners.put(joinerModel.getName(), joiner);
            } catch (ClassNotFoundException e) {
                log.error("Couldn't find the class associated with the function {}", joinerModel.getClassName());
            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Couldn't create the instance associated with the function " + joinerModel.getClassName(), e);
            }
        });
    }

    private void addStreams(PlanModel model, StreamsBuilder builder) {

        model.getQueries().entrySet().forEach(entry -> {
            Select selectQuery = entry.getValue().getSelect();

            List<String> topicStreams = selectQuery.getStreams().stream()
                    .filter(s -> !s.isTable())
                    .map(Stream::getName)
                    .collect(Collectors.toList());

            if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                topicStreams = topicStreams.stream()
                        .map(topic -> globalTopics.contains(topic) ?
                                topic : String.format("%s_%s", appId, topic))
                        .collect(Collectors.toList());
            }

            KStream<String, Map<String, Object>> stream =
                    builder.stream(topicStreams);


            List<String> dimensions = selectQuery.getDimensions();
            if (!dimensions.contains("*")) {
                stream = stream.mapValues(value -> {
                    Map<String, Object> filterValue = new HashMap<>();

                    dimensions.forEach(dim -> {
                        if (value.containsKey(dim)) {
                            filterValue.put(dim, value.get(dim));
                        }
                    });

                    return filterValue;
                });
            }

            if (config.getOrDefault(ConfigProperties.BYPASS_NULL_KEYS, false)) {
                KStream<String, Map<String, Object>> splitBranch[] = stream.branch((k, v) -> k != null, (k, v) -> k == null);

                stream = splitBranch[0];

                String outputStream = entry.getValue().getInsert().getName();
                if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                    outputStream = String.format("%s_%s", appId, outputStream);
                }

                splitBranch[1].to(outputStream);
            }

            streams.put(entry.getKey(), stream);
        });
    }

    private void addTables(PlanModel model, StreamsBuilder builder) {
        model.getQueries().entrySet().forEach(entry -> {
            List<Join> joins = entry.getValue().getJoins();

            joins.forEach(join -> {

                if (!join.getStream().isTable()) {
                    log.warn("Join beetween stream isn't supported yet! The join is changed to use stream-table join");
                }

                String tableName;
                KSTable table;

                if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                    tableName = globalTopics.contains(join.getStream().getName()) ?
                            join.getStream().getName() : String.format("%s_%s", appId, join.getStream().getName());
                } else {
                    tableName = join.getStream().getName();
                }

                if (join.getStream().isGlobalTable()) {
                    GlobalKTable<String, Map<String, Object>> globalKTable = globalTables.get(tableName);

                    if (globalKTable == null) {
                        globalKTable = builder.globalTable(tableName);
                        globalTables.put(tableName, globalKTable);
                    }

                    table = new KSTable<>(globalKTable);

                } else {
                    KTable<String, Map<String, Object>> kTable = tables.get(tableName);

                    if (kTable == null) {
                        kTable = builder.table(tableName);
                        tables.put(tableName, kTable);
                    }

                    table = new KSTable<>(kTable);
                }

                List<String> dimensions = join.getDimensions();
                if (!dimensions.contains("*")) {

                    if (table.get() instanceof KTable) {
                        KTable<String, Map<String, Object>> kTable = (KTable<String, Map<String, Object>>) table.get();

                        table.set(kTable.mapValues(value -> {
                                    Map<String, Object> filterValue = new HashMap<>();

                                    dimensions.forEach(dim -> {
                                        if (value.containsKey(dim)) {
                                            filterValue.put(dim, value.get(dim));
                                        }
                                    });

                                    return filterValue;
                                })
                        );
                    }

                }

                KStream<String, Map<String, Object>> stream = streams.get(entry.getKey());

                if (!join.getPartitionKey().equals(__KEY)) {
                    stream = stream.selectKey(ChangeKeyHelper.apply(join.getPartitionKey()));
                }

                Joiner joiner = joiners.get(join.getJoinerName());
                if (joiner == null) throw new JoinerNotFound("BaseJoiner " + join.getJoinerName() + " not found!");

                if (joiner instanceof BaseJoiner) {
                    if (table.get() instanceof KTable) {
                        KTable<String, Map<String, Object>> kTable = (KTable<String, Map<String, Object>>) table.get();
                        stream = stream.leftJoin(kTable, (BaseJoiner) joiner);
                    } else {
                        GlobalKTable<String, Map<String, Object>> globalKTable = (GlobalKTable<String, Map<String, Object>>) table.get();
                        stream = stream.leftJoin(globalKTable, ChangeKeyHelper.apply(join.getPartitionKey()), (BaseJoiner) joiner);
                    }
                } else if (joiner instanceof QueryableJoiner) {
                    KStream<String, Map<String, Object>> joinStream;

                    if (table.get() instanceof KTable) {
                        KTable<String, Map<String, Object>> kTable = (KTable<String, Map<String, Object>>) table.get();
                        joinStream = stream.leftJoin(kTable, (QueryableJoiner) joiner);
                    } else {
                        GlobalKTable<String, Map<String, Object>> globalKTable = (GlobalKTable<String, Map<String, Object>>) table.get();
                        joinStream = stream.leftJoin(globalKTable, ChangeKeyHelper.apply(join.getPartitionKey()), (QueryableJoiner) joiner);
                    }

                    joinStream
                            .branch((key, value) -> !((Boolean) value.get("joiner-status")))[0]
                            .mapValues(value -> {
                                Map<String, Object> newValue = new HashMap<>(value);
                                newValue.remove("message");
                                newValue.put("table", tableName);
                                return newValue;
                            })
                            .to("__enricher_queryable");

                    stream = joinStream.mapValues(message -> (Map<String, Object>) message.get("message"));

                } else if (joiner instanceof QueryableBackJoiner) {

                    KStream<String, Map<String, Object>> joinStream;

                    if (table.get() instanceof KTable) {
                        KTable<String, Map<String, Object>> kTable = (KTable<String, Map<String, Object>>) table.get();
                        joinStream = stream.leftJoin(kTable, (QueryableBackJoiner) joiner);
                    } else {
                        GlobalKTable<String, Map<String, Object>> globalKTable = (GlobalKTable<String, Map<String, Object>>) table.get();
                        joinStream = stream.leftJoin(globalKTable, ChangeKeyHelper.apply(join.getPartitionKey()), (QueryableBackJoiner) joiner);
                    }

                    joinStream
                            .branch((key, value) -> !((Boolean) value.get("joiner-status")))[0]
                            .mapValues(value -> {
                                Map<String, Object> newValue = new HashMap<>(value);
                                newValue.remove("message");
                                newValue.put("table", tableName);
                                return newValue;
                            })
                            .to("__enricher_queryable");

                    List<String> topics = model.getQueries().get(entry.getKey()).getSelect().getStreams().stream()
                            .filter(s -> !s.isTable())
                            .map(Stream::getName)
                            .collect(Collectors.toList());

                    //Workaround: Select the first topic to send back the data.
                    joinStream
                            .branch((key, value) -> !((Boolean) value.get("joiner-status")))[0]
                            .mapValues(message -> (Map<String, Object>) message.get("message"))
                            .to(topics.get(0));

                    stream = joinStream
                            .filter((key, value) -> (Boolean) value.get("joiner-status"))
                            .mapValues(message -> (Map<String, Object>) message.get("message"));

                }
                streams.put(entry.getKey(), stream);
            });
        });
    }

    private void addEnriches(PlanModel model) {
        model.getQueries().entrySet().forEach(entry -> {
            List<String> enrichWiths = entry.getValue().getEnrichWiths();

            enrichWiths.forEach(enrichName -> {
                KStream<String, Map<String, Object>> stream = streams.get(entry.getKey());

                Enrich enrich = enrichers.get(enrichName);
                if (enrich == null) throw new EnricherNotFound("Enricher " + enrichName + " not found!");

                if (enrich instanceof BaseEnrich) {
                    stream = stream.mapValues((BaseEnrich) enrich);
                } else {
                    log.error("WTF!! The enrich {} isn't a enrich!", enrichName);
                }

                streams.put(entry.getKey(), stream);
            });
        });
    }

    private void addInserts(PlanModel model) {
        model.getQueries().entrySet().forEach(entry -> {

            Stream insert = entry.getValue().getInsert();
            String outputPartitionKey = entry.getValue().getOutputPartitionKey();

            KStream<String, Map<String, Object>> stream = streams.get(entry.getKey());

            if (!outputPartitionKey.equals(__KEY)) {
                stream = stream.selectKey(ChangeKeyHelper.apply(outputPartitionKey));
            }

            String outputStream = insert.getName();

            if (config.getOrDefault(ConfigProperties.MULTI_ID, false)) {
                outputStream = String.format("%s_%s", appId, outputStream);
            }

            stream.to(outputStream);
        });
    }

    private void clean() {
        streams.clear();
        tables.clear();
        globalTables.clear();
        joiners.clear();
        enrichers.clear();
    }

    private <T> T makeInstance(String className)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class funcClass = Class.forName(className);
        return (T) funcClass.newInstance();
    }


    public void close() {
        clean();
    }
}
