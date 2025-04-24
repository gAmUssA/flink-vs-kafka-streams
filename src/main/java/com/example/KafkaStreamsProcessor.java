package com.example;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class KafkaStreamsProcessor {

  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsProcessor.class);

  // Topic names as constants for easier testing
  public static final String CLICKS_TOPIC = "clicks";
  public static final String CATEGORIES_TOPIC = "categories";
  public static final String OUTPUT_TOPIC = "output-topic";

  /**
   * Custom Serde for HashSet<String> to track unique user IDs
   */
  public static class HashSetSerde implements Serde<HashSet<String>> {

    @Override
    public Serializer<HashSet<String>> serializer() {
      return (topic, data) -> {
        if (data == null) {
          return null;
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
          oos.writeObject(data);
          return bos.toByteArray();
        } catch (IOException e) {
          throw new RuntimeException("Error serializing HashSet", e);
        }
      };
    }

    @Override
    public Deserializer<HashSet<String>> deserializer() {
      return (topic, data) -> {
        if (data == null) {
          return new HashSet<>();
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
          return (HashSet<String>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
          throw new RuntimeException("Error deserializing HashSet", e);
        }
      };
    }
  }

  public static void main(String[] args) {
    logger.info("Starting Kafka Streams Processor application");

    // Configure Kafka Streams
    logger.debug("Configuring Kafka Streams properties");
    Properties props = createKafkaProperties();

    // Create Avro Serdes
    logger.debug("Creating Avro Serdes");
    Map<String, Serde<?>> serdes = createSerdes(props);

    // Build the Kafka Streams topology
    logger.debug("Building Kafka Streams topology");
    Topology topology = buildTopology(serdes);

    // Start the Kafka Streams application
    logger.info("Starting Kafka Streams application");
    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();
    logger.info("Kafka Streams application started successfully");

    // Add shutdown hook to gracefully close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down Kafka Streams application");
      streams.close();
      logger.info("Kafka Streams application shut down successfully");
    }));
  }

  /**
   * Creates the Kafka Streams properties
   */
  public static Properties createKafkaProperties() {
    logger.debug("Creating Kafka Streams properties");
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clicks-processor");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
    props.put("schema.registry.url", "http://localhost:8081");
    logger.debug("Kafka Streams properties created: {}", props);
    return props;
  }

  /**
   * Creates the Avro Serdes for Click, Category, and EnrichedClick
   */
  public static Map<String, Serde<?>> createSerdes(Properties props) {
    logger.debug("Creating Avro Serdes");
    String schemaRegistryUrl = props.getProperty("schema.registry.url");
    logger.debug("Schema Registry URL: {}", schemaRegistryUrl);

    final Map<String, String> serdeConfig = Collections.singletonMap(
        "schema.registry.url", schemaRegistryUrl);

    logger.debug("Configuring Click Serde");
    final Serde<Click> clickSerde = new SpecificAvroSerde<>();
    clickSerde.configure(serdeConfig, false);

    logger.debug("Configuring Category Serde");
    final Serde<Category> categorySerde = new SpecificAvroSerde<>();
    categorySerde.configure(serdeConfig, false);

    logger.debug("Configuring EnrichedClick Serde");
    final Serde<EnrichedClick> enrichedClickSerde = new SpecificAvroSerde<>();
    enrichedClickSerde.configure(serdeConfig, false);

    logger.debug("Avro Serdes created successfully");
    return Map.of(
        "click", clickSerde,
        "category", categorySerde,
        "enrichedClick", enrichedClickSerde
    );
  }

  /**
   * Builds the Kafka Streams topology
   */
  public static Topology buildTopology(Map<String, Serde<?>> serdes) {
    logger.info("Building Kafka Streams topology");
    StreamsBuilder builder = new StreamsBuilder();

    Serde<Click> clickSerde = (Serde<Click>) serdes.get("click");
    Serde<Category> categorySerde = (Serde<Category>) serdes.get("category");
    Serde<EnrichedClick> enrichedClickSerde = (Serde<EnrichedClick>) serdes.get("enrichedClick");

    // Read clicks stream
    logger.debug("Creating clicks stream from topic: {}", CLICKS_TOPIC);
    KStream<String, Click> clicksStream = builder.stream(
        CLICKS_TOPIC,
        Consumed.with(Serdes.String(), clickSerde));

    // Read categories as a table
    logger.debug("Creating categories table from topic: {}", CATEGORIES_TOPIC);
    KTable<String, Category> categoriesTable = builder.table(
        CATEGORIES_TOPIC,
        Consumed.with(Serdes.String(), categorySerde));

    // Join clicks with categories on page_id
    logger.debug("Joining clicks with categories on page_id");
    KStream<String, EnrichedClick> enrichedClicks = clicksStream
        .selectKey((key, click) -> click.getPageId())
        .join(
            categoriesTable,
            KafkaStreamsProcessor::joinClickWithCategory,
            Joined.with(Serdes.String(), clickSerde, categorySerde)
        );

    // Group by category and count unique users in a sliding one-hour window that updates every minute
    logger.debug("Setting up windowed aggregation to count unique users per category");
    enrichedClicks
        .groupBy(
            (key, enrichedClick) -> enrichedClick.getCategory(),
            Grouped.with(Serdes.String(), enrichedClickSerde)
        )
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1))
                        .advanceBy(Duration.ofMinutes(1))) // Sliding window that updates every minute
        .aggregate(
            HashSet::new, // Initialize with empty HashSet
            (key, enrichedClick, userSet) -> {
              userSet.add(enrichedClick.getUserId()); // Add user ID to set
              logger.trace("Added user {} to category {}", enrichedClick.getUserId(), key);
              return userSet;
            },
            Materialized.<String, HashSet<String>, WindowStore<Bytes, byte[]>>as("unique-users-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(new HashSetSerde())
        )
        .toStream()
        .map((Windowed<String> key, HashSet<String> uniqueUsers) -> {
          String result = "Count: " + uniqueUsers.size() + " for window: " + key.window().startTime();
          logger.debug("Category: {}, {}", key.key(), result);
          return KeyValue.pair(key.key(), result);
        })
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    logger.info("Kafka Streams topology built successfully");
    return builder.build();
  }

  /**
   * Helper method to join a Click with a Category
   */
  public static EnrichedClick joinClickWithCategory(Click click, Category category) {
    logger.debug("Joining Click (userId={}, pageId={}) with Category (pageId={}, category={})",
                 click.getUserId(), click.getPageId(), category.getPageId(), category.getCategory());

    EnrichedClick enrichedClick = new EnrichedClick();
    enrichedClick.setUserId(click.getUserId());
    enrichedClick.setTimestamp(click.getTimestamp());
    enrichedClick.setCategory(category.getCategory());

    logger.debug("Created EnrichedClick: userId={}, timestamp={}, category={}",
                 enrichedClick.getUserId(), enrichedClick.getTimestamp(), enrichedClick.getCategory());

    return enrichedClick;
  }
}
