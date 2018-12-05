package io.wizzie.enricher.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.builder.Builder;
import io.wizzie.enricher.serializers.JsonDeserializer;
import io.wizzie.enricher.serializers.JsonSerde;
import io.wizzie.enricher.serializers.JsonSerializer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static io.wizzie.enricher.base.builder.config.ConfigProperties.BOOTSTRAPPER_CLASSNAME;
import static io.wizzie.enricher.enrichment.utils.Constants.*;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.Assert.assertEquals;

public class MacVendorEnrichIntegrationTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static MockTime MOCK_TIME = CLUSTER.time;

    private static final int REPLICATION_FACTOR = 1;

    private static final String INPUT_STREAM_TOPIC = "input";

    private static final String BOOTSTRAP_TOPIC = "__enricher_bootstrap";

    private static final String OUTPUT_TOPIC = "output";

    private static Properties producerConfig = new Properties();

    private static Properties consumerConfig = new Properties();

    private static ObjectMapper mapper;

    private static final String __CLIENT_MAC = "client_mac";

    private static final String __CLIENT_MAC_VENDOR = "mac_vendor";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_STREAM_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(OUTPUT_TOPIC, 2, REPLICATION_FACTOR);
        CLUSTER.createTopic(BOOTSTRAP_TOPIC, 1, REPLICATION_FACTOR);


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

        mapper = new ObjectMapper();
    }

    @Test
    public void macVendorEnrichShouldWork() throws Exception {
        Map<String, Object> streamsConfiguration = new HashMap<>();

        String appId = UUID.randomUUID().toString();
        streamsConfiguration.put(APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        Config configuration = new Config(streamsConfiguration);
        configuration.put(BOOTSTRAPPER_CLASSNAME, "io.wizzie.bootstrapper.bootstrappers.impl.KafkaBootstrapper");
        configuration.put(KafkaBootstrapper.BOOTSTRAP_TOPICS_CONFIG, Arrays.asList(BOOTSTRAP_TOPIC));

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Map<String, Object> macVendorProperties = new HashMap<>();
        macVendorProperties.put(OUI_FILE_PATH, classLoader.getSystemResource("mac_vendors").getPath());
        macVendorProperties.put(MAC_DIM, __CLIENT_MAC);
        macVendorProperties.put(MAC_VENDOR_DIM, __CLIENT_MAC_VENDOR);


        Map<String, Object> geoIpEnricher = new HashMap<>();
        geoIpEnricher.put("name", "macVendor");
        geoIpEnricher.put("className", "io.wizzie.enricher.enrichment.MacVendorEnrich");
        geoIpEnricher.put("properties", macVendorProperties);

        Map<String, Object> queries = new HashMap<>();
        queries.put("query1", "SELECT * FROM STREAM input ENRICH WITH macVendor INSERT INTO TABLE output");

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("enrichers", Collections.singletonList(geoIpEnricher));
        jsonMap.put("queries", queries);

        KeyValue<String, String> jsonConfigKv = new KeyValue<>(appId, mapper.writeValueAsString(jsonMap));

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(BOOTSTRAP_TOPIC, Collections.singletonList(jsonConfigKv), producerConfig, MOCK_TIME);

        Builder builder = new Builder(configuration);

        Map<String, Object> message1 = new HashMap<>();
        message1.put(__CLIENT_MAC, "00:1C:B3:09:85:15");

        KeyValue<String, Map<String, Object>> kvStream1 = new KeyValue<>("KEY_A", message1);

        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_STREAM_TOPIC, Arrays.asList(kvStream1), producerConfig, MOCK_TIME);

        List<KeyValue<String, Map>> receivedMessagesFromOutput = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, 1);

        Map<String, Object> expectedMessage = new HashMap<>();
        expectedMessage.put(__CLIENT_MAC, "00:1C:B3:09:85:15");
        expectedMessage.put(__CLIENT_MAC_VENDOR, "Apple");

        KeyValue<String, Map> expectedKVMessage = new KeyValue<>("KEY_A",expectedMessage);

        assertEquals(Collections.singletonList(expectedKVMessage), receivedMessagesFromOutput);

        builder.close();
    }

    public static String getFileContent(File file) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        StringBuilder stringBuffer = new StringBuilder();

        String line;

        while ((line = bufferedReader.readLine()) != null) {

            stringBuffer.append(line).append("\n");
        }

        return stringBuffer.toString();
    }

}
