package io.wizzie.enricher.enrichment.simple.impl;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StaticDataEnrichUnitTest {

    StaticDataEnrich staticDataEnrich;

    @Before
    public void init() {
        staticDataEnrich = new StaticDataEnrich();
    }

    @Test
    public void enrichWithStaticDataShouldWork() {

        Map<String, Object> properties = new HashMap<>();

        Map<String, Object> newData = new HashMap<>();
        newData.put("test", true);

        Map<String, Object> condition1 = new HashMap<>();
        condition1.put(StaticDataEnrich.IF_VALUE_IS_EQUAL_TO, "VALUE-A");

        condition1.put(StaticDataEnrich.ENRICH_WITH, newData);

        Map<String, Object> condition2 = new HashMap<>();
        condition2.put(StaticDataEnrich.IF_VALUE_IS_EQUAL_TO, true);
        condition2.put(StaticDataEnrich.ENRICH_WITH, newData);

        List<Map<String, Object>> dataToEnrichList = new ArrayList<>();
        dataToEnrichList.add(condition1);
        dataToEnrichList.add(condition2);

        properties.put("myDimension", dataToEnrichList);

        staticDataEnrich.init(properties, null);

        Map<String, Object> message1 = new HashMap<>();
        message1.put("myDimension", "VALUE-A");

        Map<String, Object> message2 = new HashMap<>();
        message2.put("myDimension", true);

        Map<String, Object> message3 = new HashMap<>();
        message3.put("myDimension", 15);

        Map<String, Object> expectedMessage1 = new HashMap<>(message1);
        expectedMessage1.put("test", true);

        Map<String, Object> expectedMessage2 = new HashMap<>(message2);
        expectedMessage2.put("test", true);

        assertEquals(staticDataEnrich.apply(message1), expectedMessage1);
        assertEquals(staticDataEnrich.apply(message2), expectedMessage2);
        assertEquals(staticDataEnrich.apply(message3), message3);
    }

}
