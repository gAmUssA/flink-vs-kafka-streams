package dev.gamov.streams.util;

import dev.gamov.streams.Category;
import dev.gamov.streams.Click;
import dev.gamov.streams.KafkaTCIntegrationTestBase;
import dev.gamov.streams.kafka.KafkaStreamsProcessor;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for TestDataProducer using Testcontainers.
 * This test verifies that the TestDataProducer correctly creates and produces test data to Kafka.
 */
public class TestDataProducerIntegrationTest extends KafkaTCIntegrationTestBase {

    private static final Logger logger = LoggerFactory.getLogger(TestDataProducerIntegrationTest.class);

    /**
     * Test that verifies the creation of test categories.
     */
    @Test
    public void testCreateTestCategories() {
        // Create the TestDataProducer
        String schemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" +
                                  schemaRegistryContainer.getMappedPort(8081);
        TestDataProducer testDataProducer = new TestDataProducer(
            kafkaContainer.getBootstrapServers(),
            schemaRegistryUrl
        );

        // Create test categories
        List<Category> categories = testDataProducer.createTestCategories();

        // Verify the categories
        assertThat(categories).isNotNull()
            .withFailMessage("Categories list should not be null");
        assertThat(categories.size()).isEqualTo(3)
            .withFailMessage("Should have created 3 categories");

        // Verify the first category (sports)
        Category sports = categories.get(0);
        assertThat(sports.getPageId()).isEqualTo("page1")
            .withFailMessage("First category should have page ID 'page1'");
        assertThat(sports.getCategory()).isEqualTo("sports")
            .withFailMessage("First category should be 'sports'");

        // Verify the second category (news)
        Category news = categories.get(1);
        assertThat(news.getPageId()).isEqualTo("page2")
            .withFailMessage("Second category should have page ID 'page2'");
        assertThat(news.getCategory()).isEqualTo("news")
            .withFailMessage("Second category should be 'news'");

        // Verify the third category (entertainment)
        Category entertainment = categories.get(2);
        assertThat(entertainment.getPageId()).isEqualTo("page3")
            .withFailMessage("Third category should have page ID 'page3'");
        assertThat(entertainment.getCategory()).isEqualTo("entertainment")
            .withFailMessage("Third category should be 'entertainment'");
    }

    /**
     * Test that verifies the creation of test clicks.
     */
    @Test
    public void testCreateTestClicks() {
        // Create the TestDataProducer
        String schemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" +
                                  schemaRegistryContainer.getMappedPort(8081);
        TestDataProducer testDataProducer = new TestDataProducer(
            kafkaContainer.getBootstrapServers(),
            schemaRegistryUrl
        );

        // Create test clicks
        List<Click> clicks = testDataProducer.createTestClicks();

        // Verify the clicks
        assertThat(clicks).isNotNull()
            .withFailMessage("Clicks list should not be null");
        assertThat(clicks.size()).isEqualTo(5)
            .withFailMessage("Should have created 5 clicks");

        // Verify user1 click on page1 (sports)
        Click user1Sports = clicks.get(0);
        assertThat(user1Sports.getUserId()).isEqualTo("user1")
            .withFailMessage("First click should be from user1");
        assertThat(user1Sports.getPageId()).isEqualTo("page1")
            .withFailMessage("First click should be on page1");

        // Verify user2 click on page1 (sports)
        Click user2Sports = clicks.get(1);
        assertThat(user2Sports.getUserId()).isEqualTo("user2")
            .withFailMessage("Second click should be from user2");
        assertThat(user2Sports.getPageId()).isEqualTo("page1")
            .withFailMessage("Second click should be on page1");

        // Verify user1 click on page2 (news)
        Click user1News = clicks.get(2);
        assertThat(user1News.getUserId()).isEqualTo("user1")
            .withFailMessage("Third click should be from user1");
        assertThat(user1News.getPageId()).isEqualTo("page2")
            .withFailMessage("Third click should be on page2");

        // Verify user3 click on page2 (news)
        Click user3News = clicks.get(3);
        assertThat(user3News.getUserId()).isEqualTo("user3")
            .withFailMessage("Fourth click should be from user3");
        assertThat(user3News.getPageId()).isEqualTo("page2")
            .withFailMessage("Fourth click should be on page2");

        // Verify user4 click on page3 (entertainment)
        Click user4Entertainment = clicks.get(4);
        assertThat(user4Entertainment.getUserId()).isEqualTo("user4")
            .withFailMessage("Fifth click should be from user4");
        assertThat(user4Entertainment.getPageId()).isEqualTo("page3")
            .withFailMessage("Fifth click should be on page3");
    }

    /**
     * Test that verifies producing categories to Kafka.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testProduceCategories() {
        // Create the TestDataProducer
        String schemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" +
                                  schemaRegistryContainer.getMappedPort(8081);
        TestDataProducer testDataProducer = new TestDataProducer(
            kafkaContainer.getBootstrapServers(),
            schemaRegistryUrl
        );

        // Create test categories
        List<Category> categories = testDataProducer.createTestCategories();

        // Produce categories to Kafka
        testDataProducer.produceCategories(categories);

        // Verify the categories were produced to Kafka
        verifyCategories();
    }

    /**
     * Test that verifies producing clicks to Kafka.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testProduceClicks() {
        // Create the TestDataProducer
        String schemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" +
                                  schemaRegistryContainer.getMappedPort(8081);
        TestDataProducer testDataProducer = new TestDataProducer(
            kafkaContainer.getBootstrapServers(),
            schemaRegistryUrl
        );

        // Create test clicks
        List<Click> clicks = testDataProducer.createTestClicks();

        // Produce clicks to Kafka
        testDataProducer.produceClicks(clicks);

        // Verify the clicks were produced to Kafka
        verifyClicks();
    }

    /**
     * Test that verifies producing standard test data to Kafka.
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testProduceStandardTestData() {
        // Create the TestDataProducer
        String schemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" +
                                  schemaRegistryContainer.getMappedPort(8081);
        TestDataProducer testDataProducer = new TestDataProducer(
            kafkaContainer.getBootstrapServers(),
            schemaRegistryUrl
        );

        // Produce standard test data
        testDataProducer.produceStandardTestData();

        // Verify both categories and clicks were produced to Kafka
        verifyCategories();
        verifyClicks();
    }

    /**
     * Helper method to verify that categories were produced to Kafka.
     */
    private void verifyCategories() {
        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-categories-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                         "http://" + schemaRegistryContainer.getHost() + ":" + 
                         schemaRegistryContainer.getMappedPort(8081));
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        // Create consumer and subscribe to categories topic
        try (KafkaConsumer<String, Category> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(KafkaStreamsProcessor.CATEGORIES_TOPIC));

            // Use Awaitility to wait for results with a timeout
            List<Category> categories = new ArrayList<>();

            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .pollInSameThread() // Important for Kafka Consumer
                .until(() -> {
                    ConsumerRecords<String, Category> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, Category> record : records) {
                        categories.add(record.value());
                    }
                    logger.debug("Polled {} categories so far", categories.size());
                    return categories.size() >= 3; // We expect 3 categories
                });

            // Verify we received all 3 categories
            assertThat(categories.size()).isEqualTo(3)
                .withFailMessage("Should have received 3 categories");

            // Verify the categories
            List<String> categoryNames = categories.stream()
                .map(Category::getCategory)
                .toList();
            assertThat(categoryNames).contains("sports", "news", "entertainment")
                .withFailMessage("Should have received sports, news, and entertainment categories");
        }
    }

    /**
     * Helper method to verify that clicks were produced to Kafka.
     */
    private void verifyClicks() {
        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-clicks-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                         "http://" + schemaRegistryContainer.getHost() + ":" + 
                         schemaRegistryContainer.getMappedPort(8081));
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        // Create consumer and subscribe to clicks topic
        try (KafkaConsumer<String, Click> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(KafkaStreamsProcessor.CLICKS_TOPIC));

            // Use Awaitility to wait for results with a timeout
            List<Click> clicks = new ArrayList<>();

            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .pollInSameThread() // Important for Kafka Consumer
                .until(() -> {
                    ConsumerRecords<String, Click> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, Click> record : records) {
                        clicks.add(record.value());
                    }
                    logger.debug("Polled {} clicks so far", clicks.size());
                    return clicks.size() >= 5; // We expect 5 clicks
                });

            // Verify we received all 5 clicks
            assertThat(clicks.size()).isEqualTo(5)
                .withFailMessage("Should have received 5 clicks");

            // Verify the clicks
            List<String> userIds = clicks.stream()
                .map(Click::getUserId)
                .toList();
            assertThat(userIds).contains("user1", "user2", "user3", "user4")
                .withFailMessage("Should have received clicks from user1, user2, user3, and user4");

            // Verify the page IDs
            List<String> pageIds = clicks.stream()
                .map(Click::getPageId)
                .toList();
            assertThat(pageIds).contains("page1", "page2", "page3")
                .withFailMessage("Should have received clicks on page1, page2, and page3");
        }
    }
}