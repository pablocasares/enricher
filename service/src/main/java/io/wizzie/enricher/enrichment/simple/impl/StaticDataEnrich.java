package io.wizzie.enricher.enrichment.simple.impl;

import io.wizzie.enricher.enrichment.simple.BaseEnrich;
import io.wizzie.metrics.MetricsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StaticDataEnrich extends BaseEnrich {
    private static final Logger log = LoggerFactory.getLogger(StaticDataEnrich.class);

    public static final String IF_VALUE_IS_EQUAL_TO = "ifValueIsEqualTo";
    public static final String ENRICH_WITH = "enrichWith";

    Map<String, Object> dataToEnrich = new HashMap<>();

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        for (Map.Entry<String, Object> staticData: properties.entrySet()) {
            String dimension = staticData.getKey();
            List<Map<String, Object>> conditionsList = (List<Map<String, Object>>) staticData.getValue();

            if (conditionsList != null) {
                dataToEnrich.put(dimension, conditionsList);

                for (Map<String, Object> conditionMap : conditionsList) {
                    Object ifValueIsEqualTo = conditionMap.get(IF_VALUE_IS_EQUAL_TO);
                    Object enrichWith = conditionMap.get(ENRICH_WITH);

                    if (ifValueIsEqualTo == null || enrichWith == null) {
                        log.warn(String.format("%s: <{}> %s: <{}> in dimension '{}', please check your settings. This condition will be ignored.", IF_VALUE_IS_EQUAL_TO, ENRICH_WITH), ifValueIsEqualTo, enrichWith, dimension);
                        dataToEnrich.put(dimension, new ArrayList<>());
                        break;
                    }
                }
            } else {
                log.warn("Dimension [{}] has null value in conditions. This dimension will be ignored.", dimension);
            }
        }
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> staticEnrichMap = new HashMap<>();
        staticEnrichMap.putAll(message);

        for (Map.Entry<String, Object> data : dataToEnrich.entrySet()) {
            String dimension = data.getKey();

            if (message.containsKey(dimension)) {
                List<Map<String, Object>> conditionsList = (List<Map<String, Object>>) data.getValue();
                Object dimensionValue = message.get(dimension);


                for (Map<String, Object> conditionMap : conditionsList) {
                    Object value = conditionMap.get(IF_VALUE_IS_EQUAL_TO);
                    Object enrichWith = conditionMap.get(ENRICH_WITH);

                        if (value.equals(dimensionValue)) {
                            staticEnrichMap.putAll((Map<? extends String, ?>) enrichWith);
                            break;
                        }

                }
            }
        }

        return staticEnrichMap;
    }

    @Override
    public void stop() {

    }
}
