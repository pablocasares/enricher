package io.wizzie.enricher.builder;

import com.codahale.metrics.jvm.JmxAttributeGauge;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.*;
import io.wizzie.enricher.base.utils.Utils;
import io.wizzie.enricher.model.PlanModel;
import io.wizzie.enricher.model.exceptions.PlanBuilderException;
import io.wizzie.enricher.serializers.JsonSerde;
import io.wizzie.metrics.MetricsConstant;
import io.wizzie.metrics.MetricsManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.wizzie.enricher.base.builder.config.ConfigProperties.BOOTSTRAPER_CLASSNAME;
import static org.apache.kafka.streams.StreamsConfig.*;


public class Builder implements Listener {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);
    Config config;
    StreamBuilder streamBuilder;
    KafkaStreams streams;
    MetricsManager metricsManager;
    Bootstrapper bootstrapper;

    public Builder(Config config) throws Exception {
        this.config = config;

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);

        metricsManager = new MetricsManager(config.clone().getMapConf());
        metricsManager.start();

        Map<String, Object> metricDataBag = config.getOrDefault(
                MetricsConstant.METRIC_DATABAG, new HashMap<>()
        );

        metricDataBag.put("host", Utils.getIdentifier());
        config.put(MetricsConstant.METRIC_DATABAG, metricDataBag);

        streamBuilder = new StreamBuilder(config.clone(), metricsManager);

        bootstrapper = BootstrapperBuilder.makeBuilder()
                .boostrapperClass(config.get(BOOTSTRAPER_CLASSNAME))
                .listener(this)
                .withConfigInstance(config)
                .build();
    }

    public void close() throws Exception {
        metricsManager.interrupt();
        streamBuilder.close();
        bootstrapper.close();
        if (streams != null) streams.close();
    }

    private void registerKafkaMetrics(Config config, MetricsManager metricsManager) {
        Integer streamThreads = config.getOrDefault(NUM_STREAM_THREADS_CONFIG, 1);
        String appId = config.get(APPLICATION_ID_CONFIG);


        log.info("Register kafka jvm metrics: ");

        for (int i = 1; i <= streamThreads; i++) {
            try {

                // PRODUCER
                log.info(" * {}", "producer." + i + ".messages_send_per_sec");
                metricsManager.registerMetric("producer.stream-" + i + ".messages_send_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i + "-producer"), "record-send-rate"));

                log.info(" * {}", "producer." + i + ".output_bytes_per_sec");
                metricsManager.registerMetric("producer.stream-" + i + ".output_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i + "-producer"), "outgoing-byte-rate"));

                log.info(" * {}", "producer." + i + ".incoming_bytes_per_sec");
                metricsManager.registerMetric("producer.stream-" + i + ".incoming_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.producer:type=producer-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i + "-producer"), "incoming-byte-rate"));

                // CONSUMER
                log.info(" * {}", "consumer." + i + ".max_lag");
                metricsManager.registerMetric("consumer.stream-" + i + ".max_lag",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i + "-consumer"), "records-lag-max"));

                log.info(" * {}", "consumer." + i + ".output_bytes_per_sec");
                metricsManager.registerMetric("consumer.stream-" + i + ".output_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i + "-consumer"), "outgoing-byte-rate"));

                log.info(" * {}", "consumer." + i + ".incoming_bytes_per_sec");
                metricsManager.registerMetric("consumer.stream-" + i + ".incoming_bytes_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i + "-consumer"), "incoming-byte-rate"));

                log.info(" * {}", "consumer." + i + ".records_per_sec");
                metricsManager.registerMetric("consumer.stream-" + i + ".records_per_sec",
                        new JmxAttributeGauge(new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i + "-consumer"), "records-consumed-rate"));

                // STREAMS
                log.info(" * {}", "streams.stream-" + i + ".process-latency-ms");
                metricsManager.registerMetric("streams.stream-" + i + ".process-latency-ms",
                        new JmxAttributeGauge(new ObjectName("kafka.streams:type=stream-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i), "process-latency-avg"));

                log.info(" * {}", "streams.stream-" + i + ".poll-latency-ms");
                metricsManager.registerMetric("streams.stream-" + i + ".poll-latency-ms",
                        new JmxAttributeGauge(new ObjectName("kafka.streams:type=stream-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i), "poll-latency-avg"));

                log.info(" * {}", "streams.stream-" + i + ".commit-latency-ms");
                metricsManager.registerMetric("streams.stream-" + i + ".commit-latency-ms",
                        new JmxAttributeGauge(new ObjectName("kafka.streams:type=stream-metrics,client-id="
                                + String.format("%s_%s", appId, "enricher") + "-StreamThread-"
                                + i), "commit-latency-avg"));

            } catch (MalformedObjectNameException e) {
                log.error("kafka jvm metrics not found", e);
            }
        }
    }

    @Override
    public void updateConfig(SourceSystem sourceSystem, String streamConfig) {
        if (streams != null) {
            metricsManager.clean();
            streamBuilder.close();
            streams.close();
            log.info("Clean Enricher process");
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            PlanModel model = objectMapper.readValue(streamConfig, PlanModel.class);
            log.info("Execution plan: {}", model.printExecutionPlan());
            log.info("-------- TOPOLOGY BUILD START --------");
            StreamsBuilder builder = streamBuilder.builder(model);
            log.info("--------  TOPOLOGY BUILD END  --------");

            Config configWithNewAppId = config.clone();
            String appId = configWithNewAppId.get(APPLICATION_ID_CONFIG);
            configWithNewAppId.put(APPLICATION_ID_CONFIG, String.format("%s_%s", appId, "enricher"));
            configWithNewAppId.put(CLIENT_ID_CONFIG, String.format("%s_%s", appId, "enricher"));

            Properties properties = configWithNewAppId.getProperties();

            properties.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), Integer.MAX_VALUE);
            properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), Integer.MAX_VALUE);
            properties.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, 305000);
            properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                    Integer.MAX_VALUE);

            log.info(builder.build().describe().toString());

            streams = new KafkaStreams(builder.build(), properties);
            streams.setUncaughtExceptionHandler((thread, exception) -> {
                log.error(exception.getMessage(), exception);
                log.info("Stopping enricher engine");
                try {
                    close();
                } catch (Exception e) {
                    log.error(e.getMessage(), exception);
                }
                log.info("Closing enricher engine");
            });
            streams.start();

            registerKafkaMetrics(config, metricsManager);

            log.info("Started Enricher with conf {}", config.getProperties());
        } catch (PlanBuilderException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
