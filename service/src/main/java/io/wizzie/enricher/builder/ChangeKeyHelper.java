package io.wizzie.enricher.builder;

import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Map;

import static io.wizzie.enricher.base.utils.Constants.__KEY;

public class ChangeKeyHelper {
    public static KeyValueMapper<String, Map<String, Object>, String> apply(String keyName) {
        return (key, value) -> {
            String newKey = key;

            if (!keyName.equals(__KEY)) {
                if (value != null) {
                    Object keyValue = value.get(keyName);
                    if (keyValue != null) newKey = keyValue.toString();
                }
            }

            return newKey;
        };
    }
}
