package io.wizzie.enricher.integration;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.builder.Builder;
import io.wizzie.enricher.serializers.JsonDeserializer;
import io.wizzie.enricher.serializers.JsonSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static io.wizzie.enricher.base.builder.config.ConfigProperties.BOOTSTRAPPER_CLASSNAME;
import static org.junit.Assert.assertTrue;

public class KafkaMetricListenerIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_TOPIC = "__enricher_metrics";
    private static final String DUMMY_TOPIC = "dummy_topic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // inputs
        CLUSTER.createTopic(INPUT_TOPIC, 1, REPLICATION_FACTOR);

        CLUSTER.createTopic(DUMMY_TOPIC, 1, REPLICATION_FACTOR);
    }

    @Test
    public void kafkaMetricListenerShouldWork() throws Exception {

        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        Config config = new Config(streamsConfiguration);
        config.put("metric.interval", 2000);
        config.put("metric.listeners", Collections.singletonList("io.wizzie.metrics.listeners.KafkaMetricListener"));
        config.put("metric.enable", true);
        config.put("metric.kafka.topic", INPUT_TOPIC);
        config.put("file.bootstrapper.path", Thread.currentThread().getContextClassLoader().getResource("dummy-stream.json").getFile());
        config.put(BOOTSTRAPPER_CLASSNAME, "io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper");

        Builder builder = new Builder(config);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-consumer-B");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        List<KeyValue<String, Map>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, INPUT_TOPIC, 1);

        assertTrue(!result.isEmpty());

        builder.close();
    }


}
