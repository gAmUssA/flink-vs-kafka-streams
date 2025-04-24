package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for KafkaStreamsProcessor using Testcontainers.
 * This is a sample implementation to demonstrate how to use Testcontainers for integration testing.
 */
public class KafkaStreamsIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsIntegrationTest.class);

  private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.9.0";
  private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.9.0";

  private static Network network;
  private static KafkaContainer kafkaContainer;
  private static GenericContainer<?> schemaRegistryContainer;

  @BeforeAll
  public static void startContainers() {
    logger.info("Starting containers for integration testing");

    // Create a shared network for the containers
    network = Network.newNetwork();

    // Start Kafka container
    kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
        .withNetwork(network)
        .withNetworkAliases("kafka");
    kafkaContainer.start();

    // Start Schema Registry container
    schemaRegistryContainer = new GenericContainer<>(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
        .withNetwork(network)
        .withNetworkAliases("schema-registry")
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
        .withExposedPorts(8081);
    schemaRegistryContainer.start();

    logger.info("Containers started successfully");
    logger.info("Kafka bootstrap servers: {}", kafkaContainer.getBootstrapServers());
    logger.info("Schema Registry URL: http://{}:{}",
                schemaRegistryContainer.getHost(),
                schemaRegistryContainer.getMappedPort(8081));

    // Create required topics
    createTopics();
  }

  @AfterAll
  public static void stopContainers() {
    logger.info("Stopping containers");
    if (schemaRegistryContainer != null) {
      schemaRegistryContainer.stop();
    }
    if (kafkaContainer != null) {
      kafkaContainer.stop();
    }
    if (network != null) {
      network.close();
    }
  }

  private static void createTopics() {
    try {
      Properties props = new Properties();
      props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());

      try (AdminClient adminClient = AdminClient.create(props)) {
        // Create topics with appropriate configurations
        NewTopic clicksTopic = new NewTopic(KafkaStreamsProcessor.CLICKS_TOPIC, 1, (short) 1);
        NewTopic categoriesTopic = new NewTopic(KafkaStreamsProcessor.CATEGORIES_TOPIC, 1, (short) 1);
        NewTopic outputTopic = new NewTopic(KafkaStreamsProcessor.OUTPUT_TOPIC, 1, (short) 1);

        adminClient.createTopics(Arrays.asList(clicksTopic, categoriesTopic, outputTopic)).all().get();
        logger.info("Topics created successfully");
      }
    } catch (ExecutionException | InterruptedException e) {
      logger.error("Error creating topics", e);
      throw new RuntimeException("Could not create topics", e);
    }
  }

  /**
   * Sample test that demonstrates how to use Testcontainers for integration testing.
   * This test would:
   * 1. Start a KafkaStreamsProcessor instance connected to the test containers
   * 2. Produce test data to the input topics
   * 3. Consume and verify the output from the output topic
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void testKafkaStreamsProcessorEndToEnd() throws Exception {
    logger.info("Starting end-to-end integration test");

    // Configure Kafka Streams properties to connect to the test containers
    Properties streamsProps = new Properties();
    String applicationId = "test-streams-app-" + UUID.randomUUID();
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
    streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde");
    streamsProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                     "http://" + schemaRegistryContainer.getHost() + ":" + schemaRegistryContainer.getMappedPort(8081));

    // For testing, use a smaller commit interval
    streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");

    // Create Avro Serdes
    Map<String, Serde<?>> serdes = KafkaStreamsProcessor.createSerdes(streamsProps);

    // Build the Kafka Streams topology
    Topology topology = KafkaStreamsProcessor.buildTopology(serdes);

    // Start the Kafka Streams application
    logger.info("Starting Kafka Streams application with test properties");
    KafkaStreams streams = new KafkaStreams(topology, streamsProps);

    // Use AtomicBoolean to track if we need to close the streams in finally block
    AtomicBoolean streamsStarted = new AtomicBoolean(false);

    try {
      streams.start();
      streamsStarted.set(true);
      logger.info("Kafka Streams application started successfully");

      // Produce test data to the input topics
      produceTestData();

      // Give the streams application some time to process the data
      Thread.sleep(5000);

      // Consume and verify the output
      verifyOutput();

      logger.info("End-to-end integration test completed successfully");
    } finally {
      if (streamsStarted.get()) {
        logger.info("Closing Kafka Streams application");
        streams.close();
        streams.cleanUp();
        logger.info("Kafka Streams application closed");
      }
    }
  }

  private void produceTestData() {
    logger.info("Producing test data to input topics");

    // Configure Schema Registry URL
    String schemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" +
                               schemaRegistryContainer.getMappedPort(8081);

    // Create producer properties
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    // Create Kafka producer for Click data
    try (KafkaProducer<String, Click> clickProducer = new KafkaProducer<>(producerProps);
         // Create Kafka producer for Category data
         KafkaProducer<String, Category> categoryProducer = new KafkaProducer<>(producerProps)) {

      // Create and produce Category data
      logger.info("Producing Category data");
      Category category1 = new Category();
      category1.setPageId("page1");
      category1.setCategory("sports");

      Category category2 = new Category();
      category2.setPageId("page2");
      category2.setCategory("news");

      Category category3 = new Category();
      category3.setPageId("page3");
      category3.setCategory("entertainment");

      // Send Category records
      categoryProducer.send(new ProducerRecord<>(KafkaStreamsProcessor.CATEGORIES_TOPIC,
                                                 category1.getPageId().toString(), category1)).get();
      categoryProducer.send(new ProducerRecord<>(KafkaStreamsProcessor.CATEGORIES_TOPIC,
                                                 category2.getPageId().toString(), category2)).get();
      categoryProducer.send(new ProducerRecord<>(KafkaStreamsProcessor.CATEGORIES_TOPIC,
                                                 category3.getPageId().toString(), category3)).get();

      logger.info("Category data produced successfully");

      // Create and produce Click data
      logger.info("Producing Click data");

      // Create clicks for different users on different pages
      // User1 clicks on page1 (sports)
      Click click1 = new Click();
      click1.setUserId("user1");
      click1.setTimestamp(System.currentTimeMillis());
      click1.setPageId("page1");

      // User2 clicks on page1 (sports)
      Click click2 = new Click();
      click2.setUserId("user2");
      click2.setTimestamp(System.currentTimeMillis());
      click2.setPageId("page1");

      // User1 clicks on page2 (news)
      Click click3 = new Click();
      click3.setUserId("user1");
      click3.setTimestamp(System.currentTimeMillis());
      click3.setPageId("page2");

      // User3 clicks on page2 (news)
      Click click4 = new Click();
      click4.setUserId("user3");
      click4.setTimestamp(System.currentTimeMillis());
      click4.setPageId("page2");

      // User4 clicks on page3 (entertainment)
      Click click5 = new Click();
      click5.setUserId("user4");
      click5.setTimestamp(System.currentTimeMillis());
      click5.setPageId("page3");

      // Send Click records
      clickProducer.send(new ProducerRecord<>(KafkaStreamsProcessor.CLICKS_TOPIC,
                                              click1.getUserId().toString(), click1)).get();
      clickProducer.send(new ProducerRecord<>(KafkaStreamsProcessor.CLICKS_TOPIC,
                                              click2.getUserId().toString(), click2)).get();
      clickProducer.send(new ProducerRecord<>(KafkaStreamsProcessor.CLICKS_TOPIC,
                                              click3.getUserId().toString(), click3)).get();
      clickProducer.send(new ProducerRecord<>(KafkaStreamsProcessor.CLICKS_TOPIC,
                                              click4.getUserId().toString(), click4)).get();
      clickProducer.send(new ProducerRecord<>(KafkaStreamsProcessor.CLICKS_TOPIC,
                                              click5.getUserId().toString(), click5)).get();

      logger.info("Click data produced successfully");
    } catch (Exception e) {
      logger.error("Error producing test data", e);
      throw new RuntimeException("Could not produce test data", e);
    }

    logger.info("Test data produced successfully");
  }

  private void verifyOutput() {
    logger.info("Verifying output from the output topic");

    // Configure Schema Registry URL
    String schemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" +
                               schemaRegistryContainer.getMappedPort(8081);

    // Create consumer properties
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    // Create a consumer for the output topic
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      // Subscribe to the output topic
      consumer.subscribe(Collections.singletonList(KafkaStreamsProcessor.OUTPUT_TOPIC));

      // Create a map to store the results (category -> count)
      Map<String, Integer> categoryCounts = new HashMap<>();

      // Poll for records with timeout
      int maxAttempts = 10;
      int attempt = 0;
      boolean foundAllCategories = false;

      logger.info("Polling for output records");
      while (attempt < maxAttempts && !foundAllCategories) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

        for (ConsumerRecord<String, String> record : records) {
          logger.info("Received record: key={}, value={}", record.key(), record.value());

          // Extract the count from the value (format: "Count: X for window: ...")
          String value = record.value();
          if (value.startsWith("Count: ")) {
            int countEndIndex = value.indexOf(" for window");
            if (countEndIndex > 7) { // "Count: ".length() = 7
              String countStr = value.substring(7, countEndIndex);
              try {
                int count = Integer.parseInt(countStr);
                categoryCounts.put(record.key(), count);
                logger.info("Parsed count for category {}: {}", record.key(), count);
              } catch (NumberFormatException e) {
                logger.warn("Could not parse count from value: {}", value);
              }
            }
          }
        }

        // Check if we have counts for all categories
        foundAllCategories = categoryCounts.containsKey("sports") &&
                             categoryCounts.containsKey("news") &&
                             categoryCounts.containsKey("entertainment");

        if (!foundAllCategories) {
          logger.info("Not all categories found yet, polling again (attempt {}/{})",
                      ++attempt, maxAttempts);
          Thread.sleep(1000); // Wait a bit before polling again
        }
      }

      // Verify the results
      logger.info("Verifying category counts: {}", categoryCounts);

      // We expect:
      // - 2 unique users for sports (user1, user2)
      // - 2 unique users for news (user1, user3)
      // - 1 unique user for entertainment (user4)
      assertTrue(categoryCounts.containsKey("sports"), "Sports category not found in output");
      assertEquals(2, categoryCounts.get("sports"), "Expected 2 unique users for sports");

      assertTrue(categoryCounts.containsKey("news"), "News category not found in output");
      assertEquals(2, categoryCounts.get("news"), "Expected 2 unique users for news");

      assertTrue(categoryCounts.containsKey("entertainment"), "Entertainment category not found in output");
      assertEquals(1, categoryCounts.get("entertainment"), "Expected 1 unique user for entertainment");

      logger.info("Output verification successful");
    } catch (Exception e) {
      logger.error("Error verifying output", e);
      throw new RuntimeException("Could not verify output", e);
    }
  }
}
