package io.wizzie.enricher.utils.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.model.PlanModel;
import io.wizzie.enricher.model.exceptions.PlanBuilderException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class StreamerKafkaConfig {
    public static void main(String[] args) throws IOException, PlanBuilderException {
        if (args.length == 3) {
            Properties properties = new Properties();
            properties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.put(BOOTSTRAP_SERVERS_CONFIG, args[0]);
            properties.put(ACKS_CONFIG, "1");

            BufferedReader bufferedReader = new BufferedReader(new FileReader(args[2]));

            StringBuilder stringBuffer = new StringBuilder();
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                stringBuffer.append(line).append("\n");
            }

            String streamConfig = stringBuffer.toString();
            ObjectMapper objectMapper = new ObjectMapper();
            PlanModel model = objectMapper.readValue(streamConfig, PlanModel.class);
            model.validate(new Config());

            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
            producer.send(new ProducerRecord<>("__enricher_bootstrap", 0, args[1], streamConfig),
                    ((metadata, exception) -> {
                        if (exception == null) {
                            System.out.println(String.format("Wrote stream config with appID[%s] with offset: %d",
                                    args[1], metadata.offset()));
                        } else {
                            System.out.println(exception.getMessage());
                            exception.printStackTrace();
                        }
                    })
            );

            producer.flush();
            producer.close();

            System.out.println("New executing plan: ");
            System.out.println(model.printExecutionPlan());
        } else if (args.length == 2) {
            Properties consumerConfig = new Properties();
            consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, args[0]);
            consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerConfig.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            consumerConfig.put(GROUP_ID_CONFIG, String.format(
                    "enricher-bootstrapper-%s-%s", args[1], UUID.randomUUID().toString())
            );

            KafkaConsumer<String, String> restoreConsumer = new KafkaConsumer<>(consumerConfig);
            TopicPartition storePartition = new TopicPartition("__enricher_bootstrap", 0);
            restoreConsumer.assign(Collections.singletonList(storePartition));

            // calculate the end offset of the partition
            // TODO: this is a bit hacky to first seek then position to get the end offset
            restoreConsumer.seekToEnd(singleton(storePartition));
            long endOffset = restoreConsumer.position(storePartition);

            // restore the state from the beginning of the change log otherwise
            restoreConsumer.seekToBeginning(singleton(storePartition));

            String jsonStreamConfig = null;
            long jsonOffset = 0L;
            long offset = 0L;

            while (offset < endOffset) {
                for (ConsumerRecord<String, String> record : restoreConsumer.poll(100).records(storePartition)) {
                    if (record.key().equals(args[1])) {
                        jsonStreamConfig = record.value();
                        jsonOffset = record.offset();
                    }
                }
                offset = restoreConsumer.position(storePartition);
            }

            if (jsonStreamConfig != null) {
                System.out.println(String.format("Find stream configuration with app id [%s] with offset: %d",
                        args[1], jsonOffset));
                ObjectMapper objectMapper = new ObjectMapper();
                PlanModel model = objectMapper.readValue(jsonStreamConfig, PlanModel.class);
                model.validate(new Config());

                System.out.println("Current executing plan: ");
                System.out.println(model.printExecutionPlan());

                System.out.println("Stream json config: ");
                System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(model));
            } else {
                System.out.println(String.format("Don't find any stream configuration with app id [%s]", args[1]));
            }
        } else {
            System.out.println("Usage: java -cp enricher-selfcontained.jar io.utils.bootstrap.StreamerKafkaConfig <bootstrap_kafka_servers> <app_id> [stream_config_path]");
        }
    }
}
