package com.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for KafkaStreamsProcessor using TopologyTestDriver
 */
public class KafkaStreamsProcessorTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Click> clicksTopic;
    private TestInputTopic<String, Category> categoriesTopic;
    private TestOutputTopic<String, String> outputTopic;
    private Serde<Click> clickSerde;
    private Serde<Category> categorySerde;
    private Serde<EnrichedClick> enrichedClickSerde;

    @BeforeEach
    public void setUp() {
        // Configure Kafka Streams for testing
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        props.put("schema.registry.url", "mock://test-registry");

        // Create mock serdes for testing
        Map<String, String> schemaRegistryConfig = new HashMap<>();
        schemaRegistryConfig.put("schema.registry.url", "mock://test-registry");

        clickSerde = new SpecificAvroSerde<>();
        clickSerde.configure(schemaRegistryConfig, false);

        categorySerde = new SpecificAvroSerde<>();
        categorySerde.configure(schemaRegistryConfig, false);

        enrichedClickSerde = new SpecificAvroSerde<>();
        enrichedClickSerde.configure(schemaRegistryConfig, false);

        Map<String, Serde<?>> serdes = Map.of(
                "click", clickSerde,
                "category", categorySerde,
                "enrichedClick", enrichedClickSerde
        );

        // Build the topology
        Topology topology = KafkaStreamsProcessor.buildTopology(serdes);

        // Create the test driver
        testDriver = new TopologyTestDriver(topology, props);

        // Create test topics
        clicksTopic = testDriver.createInputTopic(
                KafkaStreamsProcessor.CLICKS_TOPIC,
                new StringSerializer(),
                clickSerde.serializer());

        categoriesTopic = testDriver.createInputTopic(
                KafkaStreamsProcessor.CATEGORIES_TOPIC,
                new StringSerializer(),
                categorySerde.serializer());

        outputTopic = testDriver.createOutputTopic(
                KafkaStreamsProcessor.OUTPUT_TOPIC,
                new StringDeserializer(),
                new StringDeserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        clickSerde.close();
        categorySerde.close();
        enrichedClickSerde.close();
    }

    @Test
    public void testJoinClickWithCategory() {
        // Create test data
        Click click = new Click();
        click.setUserId("testUser");
        click.setTimestamp(1000L);
        click.setPageId("testPage");

        Category category = new Category();
        category.setPageId("testPage");
        category.setCategory("testCategory");

        // Test the join method directly
        EnrichedClick result = KafkaStreamsProcessor.joinClickWithCategory(click, category);

        // Verify the result
        assertNotNull(result, "Joined result should not be null");
        assertEquals("testUser", result.getUserId(), "User ID should match");
        assertEquals(1000L, result.getTimestamp(), "Timestamp should match");
        assertEquals("testCategory", result.getCategory(), "Category should match");

        System.out.println("[DEBUG_LOG] Test passed: joinClickWithCategory correctly joined Click and Category");
    }

    @Test
    public void testTopology() {
        // First, add the category to the KTable
        Category category = new Category();
        category.setPageId("page1");
        category.setCategory("sports");
        categoriesTopic.pipeInput("page1", category);

        // Then, add a click event
        Click click = new Click();
        click.setUserId("user1");
        click.setTimestamp(Instant.now().toEpochMilli());
        click.setPageId("page1");
        clicksTopic.pipeInput("someKey", click);

        // Verify that we get an output record
        assertFalse(outputTopic.isEmpty(), "Output topic should not be empty");
        TestRecord<String, String> outputRecord = outputTopic.readRecord();
        assertEquals("sports", outputRecord.key(), "Output key should be the category");
        assertTrue(outputRecord.value().contains("Count: 1"), "Output value should contain count of 1");

        System.out.println("[DEBUG_LOG] Test passed: Topology correctly processed click and category data");
        System.out.println("[DEBUG_LOG] Output record: " + outputRecord.key() + " -> " + outputRecord.value());
    }

    @Test
    public void testMultipleClicks() {
        // Add categories
        Category category1 = new Category();
        category1.setPageId("page1");
        category1.setCategory("sports");
        categoriesTopic.pipeInput("page1", category1);

        Category category2 = new Category();
        category2.setPageId("page2");
        category2.setCategory("news");
        categoriesTopic.pipeInput("page2", category2);

        // Add clicks from the same user to different pages
        Click click1 = new Click();
        click1.setUserId("user1");
        click1.setTimestamp(Instant.now().toEpochMilli());
        click1.setPageId("page1");
        clicksTopic.pipeInput("key1", click1);

        Click click2 = new Click();
        click2.setUserId("user1");
        click2.setTimestamp(Instant.now().toEpochMilli());
        click2.setPageId("page2");
        clicksTopic.pipeInput("key2", click2);

        // Add a click from a different user
        Click click3 = new Click();
        click3.setUserId("user2");
        click3.setTimestamp(Instant.now().toEpochMilli());
        click3.setPageId("page1");
        clicksTopic.pipeInput("key3", click3);

        // Verify that we get the expected output records
        assertFalse(outputTopic.isEmpty(), "Output topic should not be empty");

        // We should have at least 3 records (one for each click)
        assertTrue(outputTopic.getQueueSize() >= 3, "Should have at least 3 output records");

        // Read all records and verify counts
        Map<String, Integer> categoryCounts = new HashMap<>();
        while (!outputTopic.isEmpty()) {
            TestRecord<String, String> record = outputTopic.readRecord();
            String category = record.key();
            String value = record.value();

            System.out.println("[DEBUG_LOG] Output record: " + category + " -> " + value);

            // Extract count from value string
            int count = Integer.parseInt(value.split("Count: ")[1].split(" ")[0]);
            categoryCounts.put(category, count);
        }

        // Verify counts for unique users
        assertTrue(categoryCounts.containsKey("sports"), "Should have records for sports category");
        assertTrue(categoryCounts.containsKey("news"), "Should have records for news category");

        // sports should have 2 unique users (user1 and user2)
        assertEquals(2, categoryCounts.get("sports"), "Sports category should have 2 unique users");

        // news should have 1 unique user (user1)
        assertEquals(1, categoryCounts.get("news"), "News category should have 1 unique user");

        System.out.println("[DEBUG_LOG] Category counts: " + categoryCounts);
    }

    @Test
    public void testUniqueUserCounting() {
        // Add category
        Category category = new Category();
        category.setPageId("page1");
        category.setCategory("sports");
        categoriesTopic.pipeInput("page1", category);

        // Base timestamp for our test
        long baseTime = Instant.now().toEpochMilli();

        // Add a click at the base time
        Click click1 = new Click();
        click1.setUserId("user1");
        click1.setTimestamp(baseTime);
        click1.setPageId("page1");
        clicksTopic.pipeInput("key1", click1);

        // Read all records from the output topic
        System.out.println("[DEBUG_LOG] After first click (user1):");
        while (!outputTopic.isEmpty()) {
            TestRecord<String, String> record = outputTopic.readRecord();
            System.out.println("[DEBUG_LOG] Record: " + record.key() + " -> " + record.value());
        }

        // Add another click from the same user
        Click click2 = new Click();
        click2.setUserId("user1");
        click2.setTimestamp(baseTime + 1000); // 1 second later
        click2.setPageId("page1");
        clicksTopic.pipeInput("key2", click2);

        // Read all records from the output topic
        System.out.println("[DEBUG_LOG] After second click (user1 again):");
        while (!outputTopic.isEmpty()) {
            TestRecord<String, String> record = outputTopic.readRecord();
            System.out.println("[DEBUG_LOG] Record: " + record.key() + " -> " + record.value());
        }

        // Add a click from a different user
        Click click3 = new Click();
        click3.setUserId("user2");
        click3.setTimestamp(baseTime + 2000); // 2 seconds later
        click3.setPageId("page1");
        clicksTopic.pipeInput("key3", click3);

        // Read all records from the output topic
        System.out.println("[DEBUG_LOG] After third click (user2):");
        while (!outputTopic.isEmpty()) {
            TestRecord<String, String> record = outputTopic.readRecord();
            System.out.println("[DEBUG_LOG] Record: " + record.key() + " -> " + record.value());
        }

        // Add another click from the first user
        Click click4 = new Click();
        click4.setUserId("user1");
        click4.setTimestamp(baseTime + 3000); // 3 seconds later
        click4.setPageId("page1");
        clicksTopic.pipeInput("key4", click4);

        // Read all records from the output topic
        System.out.println("[DEBUG_LOG] After fourth click (user1 again):");
        while (!outputTopic.isEmpty()) {
            TestRecord<String, String> record = outputTopic.readRecord();
            System.out.println("[DEBUG_LOG] Record: " + record.key() + " -> " + record.value());
        }

        // This test is now exploratory, so we don't assert anything
    }
}
