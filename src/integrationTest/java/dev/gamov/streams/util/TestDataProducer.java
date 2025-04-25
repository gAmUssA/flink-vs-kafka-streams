package dev.gamov.streams.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import dev.gamov.streams.Category;
import dev.gamov.streams.Click;
import dev.gamov.streams.kafka.KafkaStreamsProcessor;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Utility class for creating test data and producing it to Kafka.
 * This class encapsulates the common functionality used across integration tests.
 */
public class TestDataProducer {
    private static final Logger logger = LoggerFactory.getLogger(TestDataProducer.class);

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final Properties producerProps;

    /**
     * Creates a new TestDataProducer with the given Kafka bootstrap servers and Schema Registry URL.
     *
     * @param kafkaBootstrapServers The Kafka bootstrap servers string
     * @param schemaRegistryUrl The Schema Registry URL
     */
    public TestDataProducer(String kafkaBootstrapServers, String schemaRegistryUrl) {
        this.bootstrapServers = kafkaBootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;

        // Create producer properties
        this.producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    }

    /**
     * Creates a set of standard test categories.
     *
     * @return A list of Category objects
     */
    public List<Category> createTestCategories() {
        List<Category> categories = new ArrayList<>();

        Category category1 = new Category();
        category1.setPageId("page1");
        category1.setCategory("sports");
        categories.add(category1);

        Category category2 = new Category();
        category2.setPageId("page2");
        category2.setCategory("news");
        categories.add(category2);

        Category category3 = new Category();
        category3.setPageId("page3");
        category3.setCategory("entertainment");
        categories.add(category3);

        return categories;
    }

    /**
     * Creates a set of standard test clicks.
     *
     * @return A list of Click objects
     */
    public List<Click> createTestClicks() {
        List<Click> clicks = new ArrayList<>();

        // User1 clicks on page1 (sports)
        Click click1 = new Click();
        click1.setUserId("user1");
        click1.setTimestamp(System.currentTimeMillis());
        click1.setPageId("page1");
        clicks.add(click1);

        // User2 clicks on page1 (sports)
        Click click2 = new Click();
        click2.setUserId("user2");
        click2.setTimestamp(System.currentTimeMillis());
        click2.setPageId("page1");
        clicks.add(click2);

        // User1 clicks on page2 (news)
        Click click3 = new Click();
        click3.setUserId("user1");
        click3.setTimestamp(System.currentTimeMillis());
        click3.setPageId("page2");
        clicks.add(click3);

        // User3 clicks on page2 (news)
        Click click4 = new Click();
        click4.setUserId("user3");
        click4.setTimestamp(System.currentTimeMillis());
        click4.setPageId("page2");
        clicks.add(click4);

        // User4 clicks on page3 (entertainment)
        Click click5 = new Click();
        click5.setUserId("user4");
        click5.setTimestamp(System.currentTimeMillis());
        click5.setPageId("page3");
        clicks.add(click5);

        return clicks;
    }

    /**
     * Produces the given categories to the categories topic.
     *
     * @param categories The categories to produce
     * @throws RuntimeException if there is an error producing the categories
     */
    public void produceCategories(List<Category> categories) {
        try (KafkaProducer<String, Category> producer = new KafkaProducer<>(producerProps)) {
            logger.info("Producing {} Category objects", categories.size());

            for (Category category : categories) {
                producer.send(new ProducerRecord<>(
                    KafkaStreamsProcessor.CATEGORIES_TOPIC, 
                    category.getPageId(), 
                    category
                )).get();
            }

            logger.info("Categories produced successfully");
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error producing categories", e);
            throw new RuntimeException("Failed to produce categories", e);
        }
    }

    /**
     * Produces the given clicks to the clicks topic.
     *
     * @param clicks The clicks to produce
     * @throws RuntimeException if there is an error producing the clicks
     */
    public void produceClicks(List<Click> clicks) {
        try (KafkaProducer<String, Click> producer = new KafkaProducer<>(producerProps)) {
            logger.info("Producing {} Click objects", clicks.size());

            for (Click click : clicks) {
                producer.send(new ProducerRecord<>(
                    KafkaStreamsProcessor.CLICKS_TOPIC, 
                    click.getPageId(), 
                    click
                )).get();
            }

            logger.info("Clicks produced successfully");
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error producing clicks", e);
            throw new RuntimeException("Failed to produce clicks", e);
        }
    }

    /**
     * Produces standard test data (categories and clicks) to Kafka.
     * This is a convenience method that creates and produces standard test data.
     *
     * @throws RuntimeException if there is an error producing the test data
     */
    public void produceStandardTestData() {
        logger.info("Producing standard test data to input topics");

        List<Category> categories = createTestCategories();
        List<Click> clicks = createTestClicks();

        produceCategories(categories);
        produceClicks(clicks);

        logger.info("Standard test data produced successfully");
    }

    /**
     * Gets the Kafka bootstrap servers string.
     *
     * @return The bootstrap servers string
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Gets the Schema Registry URL.
     *
     * @return The Schema Registry URL
     */
    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    /**
     * Gets the producer properties.
     *
     * @return The producer properties
     */
    public Properties getProducerProps() {
        return new Properties(producerProps);
    }


}
