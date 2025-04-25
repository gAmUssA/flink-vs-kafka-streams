package dev.gamov.streams.kafka;

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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.gamov.streams.Category;
import dev.gamov.streams.Click;
import dev.gamov.streams.KafkaTCIntegrationTestBase;
import dev.gamov.streams.util.TestDataProducer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for KafkaStreamsProcessor using Testcontainers.
 * This is a sample implementation to demonstrate how to use Testcontainers for integration testing.
 */
public class KafkaStreamsIntegrationTest extends KafkaTCIntegrationTestBase {

  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsIntegrationTest.class);

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

      // Use Awaitility to wait for data processing instead of Thread.sleep
      logger.info("Waiting for Kafka Streams to process data");

      // Consume and verify the output with Awaitility
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

    // Use the TestDataProducer utility class to produce test data
    TestDataProducer testDataProducer = new TestDataProducer(
        kafkaContainer.getBootstrapServers(), 
        schemaRegistryUrl
    );

    // Produce standard test data (categories and clicks)
    testDataProducer.produceStandardTestData();

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

      logger.info("Using Awaitility to wait for output records");

      // Use Awaitility to poll for records until all categories are found
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(Duration.ofSeconds(1))
          .pollInSameThread() // Important for Kafka Consumer which is not thread-safe
          .until(() -> {
            // Poll for records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            // Process any new records
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
            boolean foundAllCategories = categoryCounts.containsKey("sports") &&
                                         categoryCounts.containsKey("news") &&
                                         categoryCounts.containsKey("entertainment");

            if (!foundAllCategories) {
              logger.info("Not all categories found yet, will poll again");
            }

            return foundAllCategories;
          });

      // Verify the results
      logger.info("Verifying category counts: {}", categoryCounts);

      // We expect:
      // - 2 unique users for sports (user1, user2)
      // - 2 unique users for news (user1, user3)
      // - 1 unique user for entertainment (user4)
      assertThat(categoryCounts)
          .as("Category counts map").containsKey("sports")
          .withFailMessage("Sports category not found in output");

      assertThat(categoryCounts.get("sports"))
          .as("Sports unique users count")
          .isEqualTo(2)
          .withFailMessage("Expected 2 unique users for sports");

      assertThat(categoryCounts).as("Category counts map").containsKey("news")
          .withFailMessage("News category not found in output");

      assertThat(categoryCounts.get("news"))
          .as("News unique users count")
          .isEqualTo(2)
          .withFailMessage("Expected 2 unique users for news");

      assertThat(categoryCounts)
          .as("Category counts map")
          .containsKey("entertainment")
          .withFailMessage("Entertainment category not found in output");

      assertThat(categoryCounts.get("entertainment"))
          .as("Entertainment unique users count")
          .isEqualTo(1)
          .withFailMessage("Expected 1 unique user for entertainment");

      logger.info("Output verification successful");
    } catch (Exception e) {
      logger.error("Error verifying output", e);
      throw new RuntimeException("Could not verify output", e);
    }
  }
}
