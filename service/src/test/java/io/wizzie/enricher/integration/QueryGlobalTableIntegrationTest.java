package io.wizzie.enricher.integration;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.base.builder.config.ConfigProperties;
import io.wizzie.enricher.builder.Builder;
import io.wizzie.enricher.query.antlr4.Stream;
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

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class QueryGlobalTableIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final int REPLICATION_FACTOR = 1;

    private static final String GLOBAL_TOPIC = "scores";
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    private static final String appId = UUID.randomUUID().toString();

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(GLOBAL_TOPIC, 4, REPLICATION_FACTOR);
        CLUSTER.createTopic(INPUT_TOPIC, 2, REPLICATION_FACTOR);

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
    public void queryWithGlobalTableShouldWork() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();
        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        Config configuration = new Config(streamsConfiguration);
        configuration.put("file.bootstraper.path", Thread.currentThread().getContextClassLoader().getResource("query-with-global-table-integration-test.json").getFile());
        configuration.put(ConfigProperties.BOOTSTRAPER_CLASSNAME, "io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper");

        Builder builder = new Builder(configuration);

        // GLOBAL TOPIC MESSAGE 1
        Map<String, Object> globalMsg1 = new HashMap<>();
        globalMsg1.put("score", 1);

        KeyValue<String, Map<String, Object>> kvGlobal1 = new KeyValue<>("X", globalMsg1);

        // GLOBAL TOPIC MESSAGE 2
        Map<String, Object> globalMsg2 = new HashMap<>();
        globalMsg2.put("score", 2);

        KeyValue<String, Map<String, Object>> kvGlobal2 = new KeyValue<>("Y", globalMsg2);

        // GLOBAL TOPIC MESSAGE 3
        Map<String, Object> globalMsg3 = new HashMap<>();
        globalMsg3.put("score", 3);

        KeyValue<String, Map<String, Object>> kvGlobal3 = new KeyValue<>("Z", globalMsg3);

        // INPUT MESSAGE 1
        Map<String, Object> message1 = new HashMap<>();
        message1.put("a", "VALUE_A");
        message1.put("b", "VALUE_B");
        message1.put("c", "VALUE_C");

        KeyValue<String, Map<String, Object>> kvStream1 = new KeyValue<>("X", message1);

        // INPUT MESSAGE 2
        Map<String, Object> message2 = new HashMap<>();
        message2.put("u", "VALUE_U");
        message2.put("v", "VALUE_V");
        message2.put("w", "VALUE_W");

        KeyValue<String, Map<String, Object>> kvStream2 = new KeyValue<>("Y", message2);

        // INPUT MESSAGE 3
        Map<String, Object> message3 = new HashMap<>();
        message3.put("i", "VALUE_I");
        message3.put("j", "VALUE_J");
        message3.put("k", "VALUE_K");

        KeyValue<String, Map<String, Object>> kvStream3 = new KeyValue<>("Z", message3);

        // EXPECTED DATA 1 FROM OUTPUT TOPIC
        Map<String, Object> expectedData1 = new HashMap<>();
        expectedData1.put("a", "VALUE_A");
        expectedData1.put("b", "VALUE_B");
        expectedData1.put("c", "VALUE_C");
        expectedData1.put("score", 1);

        KeyValue<String, Map<String, Object>> expectedKvMessage1 = new KeyValue<>("X", expectedData1);

        // EXPECTED DATA 2 FROM OUTPUT TOPIC
        Map<String, Object> expectedData2 = new HashMap<>();
        expectedData2.put("u", "VALUE_U");
        expectedData2.put("v", "VALUE_V");
        expectedData2.put("w", "VALUE_W");
        expectedData2.put("score", 2);

        KeyValue<String, Map<String, Object>> expectedKvMessage2 = new KeyValue<>("Y", expectedData2);

        // EXPECTED DATA 3 FROM OUTPUT TOPIC
        Map<String, Object> expectedData3 = new HashMap<>();
        expectedData3.put("i", "VALUE_I");
        expectedData3.put("j", "VALUE_J");
        expectedData3.put("k", "VALUE_K");
        expectedData3.put("score", 3);

        KeyValue<String, Map<String, Object>> expectedKvMessage3 = new KeyValue<>("Z", expectedData3);


        // TEST MESSAGE 1
        IntegrationTestUtils.produceKeyValuesSynchronously(GLOBAL_TOPIC, Collections.singletonList(kvGlobal1), producerConfig, CLUSTER.time);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(kvStream1), producerConfig, CLUSTER.time);

        List<KeyValue<String, Map>> receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, INPUT_TOPIC, 1);

        assertEquals(Collections.singletonList(kvStream1), receivedMessage);


        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC,1);
        assertEquals(expectedKvMessage1, receivedMessage.get(0));

        // TEST MESSAGE 2
        IntegrationTestUtils.produceKeyValuesSynchronously(GLOBAL_TOPIC, Collections.singletonList(kvGlobal2), producerConfig, CLUSTER.time);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(kvStream2), producerConfig, CLUSTER.time);

        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, INPUT_TOPIC, 1);

        assertEquals(Collections.singletonList(kvStream2), receivedMessage);

        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        assertEquals(expectedKvMessage2, receivedMessage.get(0));

        // TEST MESSAGE 3
        IntegrationTestUtils.produceKeyValuesSynchronously(GLOBAL_TOPIC, Collections.singletonList(kvGlobal3), producerConfig, CLUSTER.time);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, Collections.singletonList(kvStream3), producerConfig, CLUSTER.time);

        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, INPUT_TOPIC, 1);

        assertEquals(Collections.singletonList(kvStream3), receivedMessage);

        receivedMessage = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        assertEquals(expectedKvMessage3, receivedMessage.get(0));

        builder.close();
    }

}
