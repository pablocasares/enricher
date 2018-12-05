package io.wizzie.enricher.integration;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.builder.Builder;
import io.wizzie.enricher.serializers.JsonDeserializer;
import io.wizzie.enricher.serializers.JsonSerde;
import io.wizzie.enricher.serializers.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class QueryableJoinsIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_FLOW_TOPIC = "flow";
    private static final String INPUT_LOCATION_TOPIC = "location";
    private static final String INPUT_REPUTATION_TOPIC = "reputation";

    private static final String QUERYABLE_TOPIC = "__enricher_queryable";

    private static final String OUTPUT_TOPIC = "output";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_FLOW_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT_LOCATION_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT_REPUTATION_TOPIC, 2, REPLICATION_FACTOR);

        CLUSTER.createTopic(QUERYABLE_TOPIC, 2, REPLICATION_FACTOR);

        CLUSTER.createTopic(OUTPUT_TOPIC, 2, REPLICATION_FACTOR);


        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void queryableJoinsShouldWork() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Config configuration = new Config(streamsConfiguration);
        configuration.put("file.bootstrapper.path", Thread.currentThread().getContextClassLoader().getResource("queryable-joins-integration-test.json").getFile());
        configuration.put(BOOTSTRAPPER_CLASSNAME, "io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper");

        Builder builder = new Builder(configuration);

        Map<String, Object> ipMessage = new HashMap<>();
        ipMessage.put("ip", "1.2.3.4");

        KeyValue<String, Map<String, Object>> kvIpMessage = new KeyValue<>("MAC_A", ipMessage);

        Map<String, Object> locMessage = new HashMap<>();
        locMessage.put("loc", "X, Y");

        KeyValue<String, Map<String, Object>> kvLocMessage = new KeyValue<>("MAC_A", locMessage);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_LOCATION_TOPIC, Collections.singletonList(kvLocMessage), producerConfig, CLUSTER.time);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_FLOW_TOPIC, Collections.singletonList(kvIpMessage), producerConfig, CLUSTER.time);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("ip", "1.2.3.4");
        expectedData.put("loc", "X, Y");

        KeyValue<String, Map<String, Object>> expectedOutputKv = new KeyValue<>("MAC_A", expectedData);

        List<KeyValue<String, Map>> receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        // STEP 1: CHECK THAT RECEIVED MESSAGE NOT CONTAINS REPUTATION INFORMATION (ONLY FLOW AND LOCATION)
        assertEquals(Collections.singletonList(expectedOutputKv), receivedMessagesFromOutput);

        List<KeyValue<String, Map>> receivedQueryableMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, QUERYABLE_TOPIC, 1);

        Map<String, Object> expectedQueryableMessage = new HashMap<>();
        expectedQueryableMessage.put("joiner", "streamQueryablePreferred");
        expectedQueryableMessage.put("type", "joiner-query");
        expectedQueryableMessage.put("table", "reputation");
        expectedQueryableMessage.put("joiner-status", false);

        KeyValue<String, Map<String, Object>> expectedQueryableKv = new KeyValue<>("MAC_A", expectedQueryableMessage);

        // STEP 2: CHECK THAT __enricher_queryable RECEIVED MESSAGE
        assertEquals(Collections.singletonList(expectedQueryableKv), receivedQueryableMessage);

        Map<String, Object> repMessage = new HashMap<>();
        repMessage.put("rep", 15);

        KeyValue<String, Map<String, Object>> kvRepMessage = new KeyValue<>("MAC_A", repMessage);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_REPUTATION_TOPIC, Collections.singletonList(kvRepMessage), producerConfig, CLUSTER.time);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_LOCATION_TOPIC, Collections.singletonList(kvLocMessage), producerConfig, CLUSTER.time);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_FLOW_TOPIC, Collections.singletonList(kvIpMessage), producerConfig, CLUSTER.time);

        receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        expectedData.put("rep", 15);

        // STEP 3: CHECK THAT RECEIVED MESSAGE CONTAINS COMPLETE INFORMATION (FLOW, LOCATION and REPUTATION)
        assertEquals(Collections.singletonList(expectedOutputKv), receivedMessagesFromOutput);

        // STEP 4: CHECK THAT __enricher_queryable NOT RECEIVED ANY MESSAGE
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, QUERYABLE_TOPIC, 0);

        builder.close();
    }

}
