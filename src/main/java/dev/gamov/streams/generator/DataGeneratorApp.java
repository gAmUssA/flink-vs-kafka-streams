package dev.gamov.streams.generator;

import dev.gamov.streams.Category;
import dev.gamov.streams.Click;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Command(
    name = "data-generator",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Generates and produces sample data to Kafka topics for the Flink vs Kafka Streams project"
)
public class DataGeneratorApp implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(DataGeneratorApp.class);

    // Topic names
    private static final String CLICKS_TOPIC = "clicks";
    private static final String CATEGORIES_TOPIC = "categories";
    private static final String ENRICHED_CLICK_TOPIC = "enrichedClick";

    @Option(names = {"-b", "--bootstrap-servers"}, description = "Kafka bootstrap servers", defaultValue = "localhost:9092")
    private String bootstrapServers;

    @Option(names = {"-s", "--schema-registry"}, description = "Schema Registry URL", defaultValue = "http://localhost:8081")
    private String schemaRegistryUrl;

    @Option(names = {"-c", "--click-count"}, description = "Number of click events to generate", defaultValue = "10")
    private int clickCount;

    @Option(names = {"-p", "--page-count"}, description = "Number of unique pages to generate", defaultValue = "5")
    private int pageCount;

    @Option(names = {"-u", "--user-count"}, description = "Number of unique users to generate", defaultValue = "3")
    private int userCount;

    @Option(names = {"--create-topics"}, description = "Create Kafka topics if they don't exist", defaultValue = "true")
    private boolean createTopics;

    @Option(names = {"--partitions"}, description = "Number of partitions for created topics", defaultValue = "1")
    private int partitions;

    @Option(names = {"--replication-factor"}, description = "Replication factor for created topics", defaultValue = "1")
    private short replicationFactor;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new DataGeneratorApp()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        logger.info("Starting data generator with configuration:");
        logger.info("  Bootstrap Servers: {}", bootstrapServers);
        logger.info("  Schema Registry URL: {}", schemaRegistryUrl);
        logger.info("  Click Count: {}", clickCount);
        logger.info("  Page Count: {}", pageCount);
        logger.info("  User Count: {}", userCount);
        logger.info("  Create Topics: {}", createTopics);

        try {
            // Create topics if requested
            if (createTopics) {
                createKafkaTopics();
            }

            // Generate and produce data
            Properties producerProps = createProducerProperties();
            List<Category> categories = generateCategories();
            List<Click> clicks = generateClicks(categories);

            produceCategories(producerProps, categories);
            produceClicks(producerProps, clicks);

            logger.info("Data generation and production completed successfully");
            return 0;
        } catch (Exception e) {
            logger.error("Error in data generator", e);
            return 1;
        }
    }

    private void createKafkaTopics() {
        logger.info("Creating Kafka topics if they don't exist");
        Properties adminProps = new Properties();
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            List<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic(CLICKS_TOPIC, partitions, replicationFactor));
            topics.add(new NewTopic(CATEGORIES_TOPIC, partitions, replicationFactor));
            topics.add(new NewTopic(ENRICHED_CLICK_TOPIC, partitions, replicationFactor));

            CreateTopicsResult result = adminClient.createTopics(topics);
            // Wait for topic creation to complete
            result.all().get(30, TimeUnit.SECONDS);
            logger.info("Topics created successfully: {}, {}, {}", CLICKS_TOPIC, CATEGORIES_TOPIC, ENRICHED_CLICK_TOPIC);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                logger.info("Topics already exist");
            } else {
                logger.warn("Error creating topics", e);
            }
        } catch (InterruptedException | TimeoutException e) {
            logger.warn("Error creating topics", e);
        }
    }

    private Properties createProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return props;
    }

    private List<Category> generateCategories() {
        logger.info("Generating {} categories", pageCount);
        List<Category> categories = new ArrayList<>();
        List<String> categoryNames = Arrays.asList("sports", "news", "entertainment", "technology", "science", "business", "health", "politics");

        for (int i = 0; i < pageCount; i++) {
            Category category = new Category();
            category.setPageId("page" + (i + 1));
            // Select a category name from the list, cycling through if needed
            category.setCategory(categoryNames.get(i % categoryNames.size()));
            categories.add(category);
            logger.debug("Generated category: pageId={}, category={}", category.getPageId(), category.getCategory());
        }

        return categories;
    }

    private List<Click> generateClicks(List<Category> categories) {
        logger.info("Generating {} clicks for {} users across {} pages", clickCount, userCount, pageCount);
        List<Click> clicks = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < clickCount; i++) {
            Click click = new Click();
            click.setUserId("user" + (random.nextInt(userCount) + 1));
            click.setTimestamp(System.currentTimeMillis());
            
            // Select a random page from the available categories
            Category randomCategory = categories.get(random.nextInt(categories.size()));
            click.setPageId(randomCategory.getPageId());
            
            clicks.add(click);
            logger.debug("Generated click: userId={}, timestamp={}, pageId={}", 
                         click.getUserId(), click.getTimestamp(), click.getPageId());
        }

        return clicks;
    }

    private void produceCategories(Properties producerProps, List<Category> categories) {
        logger.info("Producing {} categories to topic {}", categories.size(), CATEGORIES_TOPIC);
        try (KafkaProducer<String, Category> producer = new KafkaProducer<>(producerProps)) {
            for (Category category : categories) {
                producer.send(new ProducerRecord<>(
                    CATEGORIES_TOPIC,
                    category.getPageId(),
                    category
                )).get();
                logger.debug("Produced category: pageId={}, category={}", 
                             category.getPageId(), category.getCategory());
            }
            logger.info("Categories produced successfully");
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error producing categories", e);
            throw new RuntimeException("Failed to produce categories", e);
        }
    }

    private void produceClicks(Properties producerProps, List<Click> clicks) {
        logger.info("Producing {} clicks to topic {}", clicks.size(), CLICKS_TOPIC);
        try (KafkaProducer<String, Click> producer = new KafkaProducer<>(producerProps)) {
            for (Click click : clicks) {
                producer.send(new ProducerRecord<>(
                    CLICKS_TOPIC,
                    click.getPageId(),
                    click
                )).get();
                logger.debug("Produced click: userId={}, timestamp={}, pageId={}", 
                             click.getUserId(), click.getTimestamp(), click.getPageId());
            }
            logger.info("Clicks produced successfully");
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error producing clicks", e);
            throw new RuntimeException("Failed to produce clicks", e);
        }
    }
}