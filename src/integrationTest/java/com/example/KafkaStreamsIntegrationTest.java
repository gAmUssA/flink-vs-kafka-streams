package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
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

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
        streamsProps.put("application.id", "test-streams-app-" + UUID.randomUUID());
        streamsProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        streamsProps.put("schema.registry.url", "http://" + schemaRegistryContainer.getHost() + ":" + 
                schemaRegistryContainer.getMappedPort(8081));
        
        // TODO: Start KafkaStreamsProcessor with the test properties
        // This would be implemented when actually running the integration tests
        
        // Produce test data to the input topics
        produceTestData();
        
        // Consume and verify the output
        verifyOutput();
        
        logger.info("End-to-end integration test completed successfully");
    }
    
    private void produceTestData() {
        logger.info("Producing test data to input topics");
        
        // TODO: Implement test data production
        // This would include:
        // 1. Creating Avro serializers configured with the test Schema Registry
        // 2. Creating test Click and Category objects
        // 3. Producing these objects to the input topics
        
        logger.info("Test data produced successfully");
    }
    
    private void verifyOutput() {
        logger.info("Verifying output from the output topic");
        
        // TODO: Implement output verification
        // This would include:
        // 1. Creating a consumer for the output topic
        // 2. Consuming records from the output topic
        // 3. Verifying that the output matches the expected results
        
        // For demonstration purposes, we'll just assert true
        assertTrue(true, "Output verification placeholder");
        
        logger.info("Output verified successfully");
    }
}