# Flink vs Kafka Streams Project Guidelines

This document provides essential information for developers working on the Flink vs Kafka Streams comparison project.

## Build/Configuration Instructions

### Prerequisites
- Java 11 or higher
- Gradle 8.x or higher
- Apache Kafka 3.4.0
- Apache Flink 1.20.0
- Confluent Schema Registry

### Project Setup
1. Clone the repository
2. Build the project using Gradle with Kotlin DSL

### Avro Schema Generation
The project uses Avro schemas for data serialization. The schemas are located in `src/main/avro/` and are automatically compiled to Java classes during the Gradle build process.

### Schema Registry Configuration
The project requires a running Schema Registry instance. By default, it connects to `http://localhost:8081`.

### Kafka Configuration
The project requires Kafka topics for input and output data: `clicks`, `categories`, and `output-topic`.

## Testing Information

### Running Tests
The project uses JUnit 5 for testing. You can run all tests, a specific test class, or a specific test method using Gradle.

### Adding New Tests
1. Create a new test class in the `src/test/java/com/example/` directory
2. Use the JUnit 5 annotations (`@Test`, `@BeforeEach`, etc.)
3. For testing private methods, use reflection as demonstrated in `FlinkDataStreamProcessorTest`

## Additional Development Information

### Code Structure
- **Avro Schemas**: Located in `src/main/avro/`
- **Kafka Streams Implementation**: `KafkaStreamsProcessor.java`
- **Flink DataStream Implementation**: `FlinkDataStreamProcessor.java`
- **Flink Table API Implementation**: `FlinkTableProcessor.java`

### Implementation Notes

#### Kafka Streams
- Uses `SpecificAvroSerde` for Avro serialization/deserialization
- Configures Schema Registry URL in the properties
- Joins clicks stream with categories table on page_id
- Uses windowed aggregation to count unique users per category

#### Flink DataStream API
- Uses `ConfluentRegistryAvroDeserializationSchema` for Avro deserialization
- Joins clicks with categories using `KeyedCoProcessFunction` or window-based join
- Maintains state for categories using `MapState`
- Aggregates unique users using a custom `AggregateFunction`

#### Flink Table API
- Defines tables using `TableSource` with `KafkaTableSourceSinkFactory`
- Specifies Avro-Confluent format and Schema Registry
- Uses builder-style API for joins, windowing, and aggregations

### Common Issues and Solutions

#### Jackson Dependencies
If you encounter `NoClassDefFoundError` for Jackson classes, ensure you have the appropriate Jackson dependencies in your build.gradle.kts.

#### Avro Class Generation
If Avro classes are not generated correctly, check:
1. The Avro schema files in `src/main/avro/`
2. The Avro plugin configuration in build.gradle.kts
3. Run `./gradlew clean generateAvroJava` to regenerate the classes

#### Kafka Streams TimeWindows
In Kafka Streams 3.4.0, use `TimeWindows.ofSizeWithNoGrace()` instead of `TimeWindows.of()`

### Performance Considerations
- **Kafka Streams**: Scales horizontally by adding more instances, limited by the number of partitions
- **Flink**: Scales both vertically and horizontally, with more flexible resource allocation
- For large-scale processing, consider:
  - Increasing parallelism in Flink
  - Increasing the number of partitions in Kafka
  - Using RocksDB state backend for large state in both frameworks
