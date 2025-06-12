package dev.gamov.streams.flink;

import dev.gamov.streams.KafkaTCIntegrationTestBase;
import dev.gamov.streams.util.TestDataProducer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
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
 * Integration test for FlinkTableApiProcessor using Testcontainers.
 * This test extends KafkaTCIntegrationTestBase which provides the Kafka and Schema Registry infrastructure.
 */
@Disabled
public class FlinkTableApiProcessorIntegrationTest extends KafkaTCIntegrationTestBase {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTableApiProcessorIntegrationTest.class);

    /**
     * Test that demonstrates the FlinkTableApiProcessor functionality.
     * This test:
     * 1. Sets up a Flink TableEnvironment
     * 2. Configures the FlinkTableApiProcessor to connect to the test containers
     * 3. Produces test data to the input topics
     * 4. Executes the Flink job
     * 5. Verifies the output from the output topic
     */
    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    public void testFlinkTableApiProcessorEndToEnd() throws Exception {
        logger.info("Starting end-to-end integration test for Flink Table API");

        // Set up Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Set up TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Configure Flink to connect to the test containers
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        String schemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" +
                                   schemaRegistryContainer.getMappedPort(8081);

        logger.info("Configuring Flink with bootstrap servers: {}", bootstrapServers);
        logger.info("Configuring Flink with schema registry URL: {}", schemaRegistryUrl);

        // Create properties for Flink
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("schema.registry.url", schemaRegistryUrl);

        // Produce test data to the input topics
        produceTestData();

        // Configure and execute the Flink job
        try {
            // Process data using Table API
            logger.info("Processing data using Table API");
            FlinkTableApiProcessor.processData(tableEnv, properties);

            // Execute the Flink job (detached mode for testing)
            logger.info("Executing Flink job");
            env.executeAsync("Flink Table API Integration Test");

            // Wait for job to process data
            logger.info("Waiting for job to process data");
            Thread.sleep(15000); // Give the job time to process data and produce results

            // Verify the output
            verifyOutput();

            logger.info("End-to-end integration test for Flink Table API completed successfully");
        } catch (Exception e) {
            logger.error("Error during Flink job execution", e);
            throw e;
        }
    }

    private void produceTestData() {
        logger.info("Producing test data to input topics");

        // Construct the schema registry URL
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

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-table-test-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                          org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                          org.apache.kafka.common.serialization.StringDeserializer.class.getName());

        // Create consumer and subscribe to output topic
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(FlinkTableApiProcessor.OUTPUT_TOPIC));

            // Use Awaitility to wait for results with a timeout
            List<ConsumerRecord<String, String>> records = new ArrayList<>();

            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .pollInSameThread() // Important for Kafka Consumer
                .until(() -> {
                    consumer.poll(Duration.ofMillis(100))
                        .forEach(records::add);
                    logger.debug("Polled {} records so far", records.size());
                    return !records.isEmpty();
                });

            // Verify the results
            logger.info("Received {} records from output topic", records.size());
            assertThat(records).isNotEmpty()
                .withFailMessage("No records received from output topic");

            // Verify that each record contains expected data
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                
                // Verify that the value contains "unique users" which is part of the output format
                assertThat(value).contains("unique users")
                    .withFailMessage("Expected record value to contain 'unique users'");
                
                logger.info("Category: {}, Value: {}", key, value);
            }
        }

        logger.info("Output verified successfully");
    }
}