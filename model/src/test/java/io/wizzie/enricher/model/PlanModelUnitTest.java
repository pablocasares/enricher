package io.wizzie.enricher.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.base.builder.config.ConfigProperties;
import io.wizzie.enricher.model.exceptions.MaxOutputKafkaTopics;
import io.wizzie.enricher.model.exceptions.PlanBuilderException;
import io.wizzie.enricher.query.antlr4.Join;
import io.wizzie.enricher.query.antlr4.Query;
import io.wizzie.enricher.query.antlr4.Select;
import io.wizzie.enricher.query.antlr4.Stream;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PlanModelUnitTest {

    private static ObjectMapper mapper;

    @BeforeClass
    public static void initTest() {
        mapper = new ObjectMapper();
    }

    @Test
    public void planModelShouldBeBuiltCorrectly() throws IOException, PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("plan-builder.json").getFile());
        PlanModel planModel = mapper.readValue(file, PlanModel.class);

        assertNotNull(planModel);

        planModel.validate(new Config());

        Map<String, Query> queries = planModel.getQueries();

        assertFalse(queries.isEmpty());
        assertEquals(3, queries.size());

        // *********************************************** Query1 *********************************************//
        Query query1 = queries.get("query1");

        assertNotNull(query1);

        // ******************************************* Query1: Select *********************************************//
        Select select1_1 = query1.getSelect();

        assertNotNull(select1_1);

        List<String> select1_1Dimensions = select1_1.getDimensions();

        assertNotNull(select1_1Dimensions);
        assertEquals(1, select1_1Dimensions.size());
        assertEquals(Collections.singletonList("*"), select1_1Dimensions);

        List<Stream> select1_1Streams = select1_1.getStreams();

        assertNotNull(select1_1Streams);
        assertEquals(1, select1_1Streams.size());

        Stream stream1_1 = select1_1Streams.get(0);

        assertEquals("rb_input", stream1_1.getName());
        assertFalse(stream1_1.isTable());

        // ******************************************* Query1: Joins *********************************************//
        List<Join> joins1_1 = query1.getJoins();

        assertNotNull(joins1_1);
        assertEquals(1, joins1_1.size());

        Join join1_1 = joins1_1.get(0);

        List<String> join1_1Dimensions = join1_1.getDimensions();

        assertNotNull(join1_1Dimensions);

        assertEquals(2, join1_1Dimensions.size());
        assertEquals(Arrays.asList("a", "b"), join1_1Dimensions);

        Stream join1_1Stream = join1_1.getStream();

        assertNotNull(join1_1Stream);
        assertEquals("rb_input2", join1_1Stream.getName());
        assertFalse(join1_1Stream.isTable());

        String join1_1ClassName = join1_1.getJoinerName();

        assertNotNull(join1_1ClassName);
        assertEquals("joiner", join1_1ClassName);

        // ********************************************** Query1: Enrich *********************************************//

        List<String> enrichers = query1.getEnrichWiths();

        assertNotNull(enrichers);

        String enricher = enrichers.get(0);
        assertNotNull(enricher);
        assertEquals("enricher", enricher);

        // ********************************************** Query1: Insert *********************************************//
        Stream insert1_1 = query1.getInsert();
        assertNotNull(insert1_1);
        assertEquals("rb_output", insert1_1.getName());
        assertTrue(insert1_1.isTable());


        // *********************************************** Query2 *********************************************//
        Query query2 = queries.get("query2");
        assertNotNull(query2);

        // ******************************************* Query2: Select *********************************************//
        Select select2_1 = query2.getSelect();
        assertNotNull(select2_1);

        List<String> select2_1Dimensions = select2_1.getDimensions();
        assertNotNull(select2_1Dimensions);
        assertEquals(4, select2_1Dimensions.size());
        assertEquals(Arrays.asList("i","j","k","l"), select2_1Dimensions);

        List<Stream> select2_1Streams = select2_1.getStreams();
        assertNotNull(select2_1Streams);
        assertEquals(2, select2_1Streams.size());

        Stream stream2_1 = select2_1Streams.get(0);
        assertEquals("rb_input", stream2_1.getName());
        assertFalse(stream2_1.isTable());

        Stream stream2_2 = select2_1Streams.get(1);
        assertEquals("rb_input2", stream2_2.getName());
        assertFalse(stream2_2.isTable());

        // *********************************************** Query2: Joins *********************************************//
        List<Join> joins2_1 = query2.getJoins();

        assertNotNull(joins2_1);
        assertEquals(3, joins2_1.size());

        // Join2.1
        Join join2_1 = joins2_1.get(0);

        List<String> join2_1Dimensions = join2_1.getDimensions();

        assertNotNull(join2_1Dimensions);

        assertEquals(3, join2_1Dimensions.size());
        assertEquals(Arrays.asList("a", "b", "c"), join2_1Dimensions);

        Stream join2_1Stream = join2_1.getStream();

        assertNotNull(join2_1Stream);
        assertEquals("rb_input3", join2_1Stream.getName());
        assertFalse(join2_1Stream.isTable());

        String join2_1ClassName = join2_1.getJoinerName();

        assertNotNull(join2_1ClassName);
        assertEquals("joiner1", join2_1ClassName);

        // Join2.2
        Join join2_2 = joins2_1.get(1);

        List<String> join2_2Dimensions = join2_2.getDimensions();

        assertNotNull(join2_2Dimensions);

        assertEquals(1, join2_2Dimensions.size());
        assertEquals(Collections.singletonList("*"), join2_2Dimensions);

        Stream join2_2Stream = join2_2.getStream();

        assertNotNull(join2_2Stream);
        assertEquals("rb_input4", join2_2Stream.getName());
        assertTrue(join2_2Stream.isTable());

        String join2_2ClassName = join2_2.getJoinerName();

        assertNotNull(join2_2ClassName);
        assertEquals("joinerPkg2Class2", join2_2ClassName);

        // Join2.3
        Join join2_3 = joins2_1.get(2);

        List<String> join2_3Dimensions = join2_3.getDimensions();

        assertNotNull(join2_3Dimensions);

        assertEquals(2, join2_3Dimensions.size());
        assertEquals(Arrays.asList("x", "y"), join2_3Dimensions);

        Stream join2_3Stream = join2_3.getStream();

        assertNotNull(join2_3Stream);
        assertEquals("rb_input5", join2_3Stream.getName());
        assertFalse(join2_3Stream.isTable());

        String join2_3ClassName = join2_3.getJoinerName();

        assertNotNull(join2_3ClassName);
        assertEquals("jClass3", join2_3ClassName);

        // ********************************************** Query2: Enrich *********************************************//

        List<String> enrichers2 = query2.getEnrichWiths();

        assertNotNull(enrichers2);

        String enricher2_1 = enrichers2.get(0);
        assertNotNull(enricher2_1);
        assertEquals("enricher1", enricher2_1);

        // ********************************************** Query2: Insert *********************************************//
        Stream insert2_1 = query2.getInsert();
        assertNotNull(insert2_1);
        assertEquals("rb_output", insert2_1.getName());
        assertTrue(insert2_1.isTable());

        // *********************************************** Query3 *********************************************//
        Query query3 = queries.get("query3");
        assertNotNull(query3);

        // ******************************************* Query3: Select *********************************************//
        Select select3_1 = query3.getSelect();
        assertNotNull(select3_1);

        List<String> select3_1Dimensions = select3_1.getDimensions();

        assertNotNull(select3_1Dimensions);
        assertEquals(1, select3_1Dimensions.size());
        assertEquals(Collections.singletonList("*"), select3_1Dimensions);

        List<Stream> select3_1Streams = select3_1.getStreams();

        assertNotNull(select3_1Streams);
        assertEquals(1, select3_1Streams.size());

        Stream stream3_1 = select3_1Streams.get(0);

        assertEquals("rb_input", stream3_1.getName());
        assertFalse(stream3_1.isTable());

        // ******************************************* Query3: Joins *********************************************//
        List<Join> joins3_1 = query3.getJoins();

        assertNotNull(joins3_1);
        assertEquals(2, joins3_1.size());

        Join join3_1 = joins3_1.get(0);

        List<String> join3_1Dimensions = join3_1.getDimensions();
        assertNotNull(join3_1Dimensions);
        assertEquals(3, join3_1Dimensions.size());
        assertEquals(Arrays.asList("a", "b", "c"), join3_1Dimensions);

        Stream join3_1Stream = join3_1.getStream();

        assertNotNull(join3_1Stream);
        assertEquals("rb_input2", join3_1Stream.getName());
        assertTrue(join3_1Stream.isTable());

        String join3_1ClassName = join3_1.getJoinerName();

        assertNotNull(join3_1ClassName);
        assertEquals("joiner1", join3_1ClassName);

        Join join3_2 = joins3_1.get(1);

        List<String> join3_2Dimensions = join3_2.getDimensions();
        assertNotNull(join3_2Dimensions);
        assertEquals(1, join3_2Dimensions.size());
        assertEquals(Collections.singletonList("*"), join3_2Dimensions);

        Stream join3_2Stream = join3_2.getStream();

        assertNotNull(join3_2Stream);
        assertEquals("rb_input3", join3_2Stream.getName());
        assertFalse(join3_2Stream.isTable());

        String join3_2ClassName = join3_2.getJoinerName();

        assertNotNull(join3_2ClassName);
        assertEquals("jClass2", join3_2ClassName);

        // ********************************************** Query1: Enrich *********************************************//

        List<String> enrichers3 = query3.getEnrichWiths();

        assertNotNull(enrichers3);

        String enricher3_1 = enrichers3.get(0);
        assertNotNull(enricher3_1);
        assertEquals("enricherPkg1Class2", enricher3_1);


        // ********************************************** Query3: Insert *********************************************//
        Stream insert3_1 = query3.getInsert();
        assertNotNull(insert3_1);
        assertEquals("rb_output", insert3_1.getName());
        assertFalse(insert3_1.isTable());
    }

    @Test(expected = MaxOutputKafkaTopics.class)
    public void throwExceptionMaxOutputKafkaTopicsMultipleStreams() throws PlanBuilderException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("plan-builder.json").getFile());
        ObjectMapper objectMapper = new ObjectMapper();

        PlanModel model = null;

        try {
            model = objectMapper.readValue(file, PlanModel.class);
        } catch (IOException e) {
            fail("Exception : " + e.getMessage());
        }

        Config config = new Config();
        config.put(ConfigProperties.MAX_KAFKA_OUTPUT_TOPICS, 1);
        model.validate(config);
    }
}
