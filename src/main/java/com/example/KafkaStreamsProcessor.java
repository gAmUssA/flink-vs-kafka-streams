package com.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.common.utils.Bytes;

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
import java.util.Set;

public class KafkaStreamsProcessor {

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
            return new Serializer<HashSet<String>>() {
                @Override
                public byte[] serialize(String topic, HashSet<String> data) {
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
                }
            };
        }

        @Override
        public Deserializer<HashSet<String>> deserializer() {
            return new Deserializer<HashSet<String>>() {
                @SuppressWarnings("unchecked")
                @Override
                public HashSet<String> deserialize(String topic, byte[] data) {
                    if (data == null) {
                        return new HashSet<>();
                    }
                    try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                         ObjectInputStream ois = new ObjectInputStream(bis)) {
                        return (HashSet<String>) ois.readObject();
                    } catch (IOException | ClassNotFoundException e) {
                        throw new RuntimeException("Error deserializing HashSet", e);
                    }
                }
            };
        }
    }

    public static void main(String[] args) {
        // Configure Kafka Streams
        Properties props = createKafkaProperties();

        // Create Avro Serdes
        Map<String, Serde<?>> serdes = createSerdes(props);

        // Build the Kafka Streams topology
        Topology topology = buildTopology(serdes);

        // Start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Creates the Kafka Streams properties
     */
    public static Properties createKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clicks-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        return props;
    }

    /**
     * Creates the Avro Serdes for Click, Category, and EnrichedClick
     */
    public static Map<String, Serde<?>> createSerdes(Properties props) {
        String schemaRegistryUrl = props.getProperty("schema.registry.url");
        final Map<String, String> serdeConfig = Collections.singletonMap(
                "schema.registry.url", schemaRegistryUrl);

        final Serde<Click> clickSerde = new SpecificAvroSerde<>();
        clickSerde.configure(serdeConfig, false);

        final Serde<Category> categorySerde = new SpecificAvroSerde<>();
        categorySerde.configure(serdeConfig, false);

        final Serde<EnrichedClick> enrichedClickSerde = new SpecificAvroSerde<>();
        enrichedClickSerde.configure(serdeConfig, false);

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
        StreamsBuilder builder = new StreamsBuilder();

        Serde<Click> clickSerde = (Serde<Click>) serdes.get("click");
        Serde<Category> categorySerde = (Serde<Category>) serdes.get("category");
        Serde<EnrichedClick> enrichedClickSerde = (Serde<EnrichedClick>) serdes.get("enrichedClick");

        // Read clicks stream
        KStream<String, Click> clicksStream = builder.stream(
                CLICKS_TOPIC,
                Consumed.with(Serdes.String(), clickSerde));

        // Read categories as a table
        KTable<String, Category> categoriesTable = builder.table(
                CATEGORIES_TOPIC,
                Consumed.with(Serdes.String(), categorySerde));

        // Join clicks with categories on page_id
        KStream<String, EnrichedClick> enrichedClicks = clicksStream
                .selectKey((key, click) -> click.getPageId())
                .join(
                        categoriesTable,
                        (click, category) -> joinClickWithCategory(click, category),
                        Joined.with(Serdes.String(), clickSerde, categorySerde)
                );

        // Group by category and count unique users in a sliding one-hour window that updates every minute
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
                            return userSet;
                        },
                        Materialized.<String, HashSet<String>, WindowStore<Bytes, byte[]>>as("unique-users-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new HashSetSerde())
                )
                .toStream()
                .map((Windowed<String> key, HashSet<String> uniqueUsers) ->
                        KeyValue.pair(key.key(), "Count: " + uniqueUsers.size() + " for window: " + key.window().startTime()))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    /**
     * Helper method to join a Click with a Category
     */
    public static EnrichedClick joinClickWithCategory(Click click, Category category) {
        EnrichedClick enrichedClick = new EnrichedClick();
        enrichedClick.setUserId(click.getUserId());
        enrichedClick.setTimestamp(click.getTimestamp());
        enrichedClick.setCategory(category.getCategory());
        return enrichedClick;
    }
}
