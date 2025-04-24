package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTCIntegrationTestBase {

  protected static final Logger logger = LoggerFactory.getLogger(KafkaStreamsIntegrationTest.class);
  private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.9.0";
  private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.9.0";
  protected static KafkaContainer kafkaContainer;
  protected static GenericContainer<?> schemaRegistryContainer;
  private static Network network;

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
    KafkaTCIntegrationTestBase.createTopics();
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
}
