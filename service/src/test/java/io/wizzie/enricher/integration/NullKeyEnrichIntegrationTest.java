package io.wizzie.enricher.integration;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.base.builder.config.ConfigProperties;
import io.wizzie.enricher.builder.Builder;
import io.wizzie.enricher.serializers.JsonDeserializer;
import io.wizzie.enricher.serializers.JsonSerde;
import io.wizzie.enricher.serializers.JsonSerializer;
import kafka.utils.MockTime;
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

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NullKeyEnrichIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT1_STREAM_TOPIC = "input1";
    private static final String INPUT2_STREAM_TOPIC = "input2";
    private static final String INPUT3_STREAM_TOPIC = "input3";

    private static final String OUTPUT_TOPIC = "output";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        // Inputs
        CLUSTER.createTopic(INPUT1_STREAM_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT2_STREAM_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT3_STREAM_TOPIC, 2, REPLICATION_FACTOR);

        // Output
        CLUSTER.createTopic(OUTPUT_TOPIC, 2, REPLICATION_FACTOR);

        // Producer config
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Consumer config
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void staticDataEnrichShouldWork() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        Config configuration = new Config(streamsConfiguration);
        configuration.put("file.bootstrapper.path", Thread.currentThread().getContextClassLoader().getResource("static-data-enrich-integration-test.json").getFile());
        configuration.put(ConfigProperties.BOOTSTRAPPER_CLASSNAME, "io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper");

        Builder builder = new Builder(configuration);

        // Input 1 message 1

        Map<String, Object> input1Msg1 = new HashMap<>();
        input1Msg1.put("timestamp", 1234567890);
        input1Msg1.put("dimension-1", true);

        KeyValue<String, Map<String, Object>> kvStream11 = new KeyValue<>(null, input1Msg1);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT1_STREAM_TOPIC, Arrays.asList(kvStream11), producerConfig, MOCK_TIME);

        Map<String, Object> expectedMsg11 = new HashMap<>(input1Msg1);
        expectedMsg11.put("response", "YES");

        KeyValue<String, Map<String, Object>> expectedKvStream11 = new KeyValue<>(null, expectedMsg11);

        List<KeyValue<String, Map>> receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        assertTrue(receivedMessagesFromOutput.size() > 0);
        assertEquals(expectedKvStream11, receivedMessagesFromOutput.get(0));

        // Input 1 message 2
        Map<String, Object> input1Msg2 = new HashMap<>();
        input1Msg2.put("timestamp", 1234568890);
        input1Msg2.put("dimension-1", false);

        KeyValue<String, Map<String, Object>> kvStream12 = new KeyValue<>(null, input1Msg2);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT1_STREAM_TOPIC, Arrays.asList(kvStream12), producerConfig, MOCK_TIME);

        Map<String, Object> expectedMsg12 = new HashMap<>(input1Msg2);
        expectedMsg12.put("response", "NO");

        KeyValue<String, Map<String, Object>> expectedKvStream12 = new KeyValue<>(null, expectedMsg12);

        receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        assertTrue(receivedMessagesFromOutput.size() > 0);
        assertEquals(expectedKvStream12, receivedMessagesFromOutput.get(0));

        // Input 2

        Map<String, Object> input2Msg1 = new HashMap<>();
        input2Msg1.put("timestamp", 1234569890);
        input2Msg1.put("dimension-2", "one");

        KeyValue<String, Map<String, Object>> kvStream21 = new KeyValue<>(null, input2Msg1);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT2_STREAM_TOPIC, Arrays.asList(kvStream21), producerConfig, MOCK_TIME);

        Map<String, Object> expectedMsg21 = new HashMap<>(input2Msg1);
        expectedMsg21.put("digit", 1);

        KeyValue<String, Map<String, Object>> expectedKvStream21 = new KeyValue<>(null, expectedMsg21);

        receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        assertTrue(receivedMessagesFromOutput.size() > 0);
        assertEquals(expectedKvStream21, receivedMessagesFromOutput.get(0));

        Map<String, Object> input2Msg2 = new HashMap<>();
        input2Msg2.put("timestamp", 1234570890);
        input2Msg2.put("dimension-2", "two");

        KeyValue<String, Map<String, Object>> kvStream22 = new KeyValue<>(null, input2Msg2);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT2_STREAM_TOPIC, Arrays.asList(kvStream22), producerConfig, MOCK_TIME);

        Map<String, Object> expectedMsg22 = new HashMap<>(input2Msg2);
        expectedMsg22.put("digit", 2);

        KeyValue<String, Map<String, Object>> expectedKvStream22 = new KeyValue<>(null, expectedMsg22);

        receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        assertTrue(receivedMessagesFromOutput.size() > 0);
        assertEquals(expectedKvStream22, receivedMessagesFromOutput.get(0));

        // Input 3

        Map<String, Object> input3Msg1 = new HashMap<>();
        input3Msg1.put("timestamp", 1234571890);
        input3Msg1.put("dimension-3", null);

        KeyValue<String, Map<String, Object>> kvStream31 = new KeyValue<>(null, input3Msg1);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT3_STREAM_TOPIC, Arrays.asList(kvStream31), producerConfig, MOCK_TIME);

        Map<String, Object> expectedMsg31 = new HashMap<>(input3Msg1);

        KeyValue<String, Map<String, Object>> expectedKvStream31 = new KeyValue<>(null, expectedMsg31);

        receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        assertTrue(receivedMessagesFromOutput.size() > 0);
        assertEquals(expectedKvStream31, receivedMessagesFromOutput.get(0));

        builder.close();
    }

}
