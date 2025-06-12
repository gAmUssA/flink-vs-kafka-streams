# Flink vs Kafka Streams Data Generator

This is a small data generator application that produces sample data to Kafka topics for the Flink vs Kafka Streams comparison project.

## Features

- Generates sample Click and Category data based on Avro schemas
- Produces data to Kafka topics (clicks, category)
- Creates required Kafka topics if they don't exist (clicks, category, enrichedClick)
- Configurable number of clicks, pages, and users
- Command-line interface with various options

## Prerequisites

- Java 17 or higher
- Gradle 8.x or higher
- Apache Kafka 3.4.0 or higher
- Confluent Schema Registry

## Building and Running with Make

The project includes a Makefile to simplify common tasks:

```bash
# Show available commands
make help

# Build the application
make build

# Run with default settings
make run

# Run with custom settings
make run-custom CLICK_COUNT=50 USER_COUNT=5

# Generate a large dataset
make run-large

# Run without creating topics
make run-no-topics
```

## Building Manually

```bash
./gradlew clean build
```

## Running Manually

```bash
java -cp build/libs/flink-vs-kafka-streams-1.0-SNAPSHOT.jar dev.gamov.streams.generator.DataGeneratorApp
```

## Command-Line Options

The data generator supports the following command-line options:

```
Usage: data-generator [-hV] [-b=<bootstrapServers>] [-c=<clickCount>]
                      [--create-topics=<createTopics>] [-p=<pageCount>]
                      [--partitions=<partitions>]
                      [--replication-factor=<replicationFactor>]
                      [-s=<schemaRegistryUrl>] [-u=<userCount>]
Generates and produces sample data to Kafka topics for the Flink vs Kafka Streams
project
  -b, --bootstrap-servers=<bootstrapServers>
                          Kafka bootstrap servers
                          Default: localhost:9092
  -c, --click-count=<clickCount>
                          Number of click events to generate
                          Default: 10
      --create-topics=<createTopics>
                          Create Kafka topics if they don't exist
                          Default: true
  -h, --help              Show this help message and exit.
  -p, --page-count=<pageCount>
                          Number of unique pages to generate
                          Default: 5
      --partitions=<partitions>
                          Number of partitions for created topics
                          Default: 1
      --replication-factor=<replicationFactor>
                          Replication factor for created topics
                          Default: 1
  -s, --schema-registry=<schemaRegistryUrl>
                          Schema Registry URL
                          Default: http://localhost:8081
  -u, --user-count=<userCount>
                          Number of unique users to generate
                          Default: 3
  -V, --version           Print version information and exit.
```

## Examples

### Generate default data

```bash
java -cp build/libs/flink-vs-kafka-streams-1.0-SNAPSHOT.jar dev.gamov.streams.generator.DataGeneratorApp
```

### Generate 100 clicks for 10 users across 8 pages

```bash
java -cp build/libs/flink-vs-kafka-streams-1.0-SNAPSHOT.jar dev.gamov.streams.generator.DataGeneratorApp -c 100 -u 10 -p 8
```

### Connect to a specific Kafka cluster and Schema Registry

```bash
java -cp build/libs/flink-vs-kafka-streams-1.0-SNAPSHOT.jar dev.gamov.streams.generator.DataGeneratorApp -b kafka1:9092,kafka2:9092 -s http://schema-registry:8081
```

### Generate data without creating topics

```bash
java -cp build/libs/flink-vs-kafka-streams-1.0-SNAPSHOT.jar dev.gamov.streams.generator.DataGeneratorApp --create-topics=false
```

## Data Schema

### Click Schema
```json
{
  "type": "record",
  "name": "Click",
  "namespace": "dev.gamov.streams",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "page_id", "type": "string"}
  ]
}
```

### Category Schema
```json
{
  "type": "record",
  "name": "Category",
  "namespace": "dev.gamov.streams",
  "fields": [
    {"name": "page_id", "type": "string"},
    {"name": "category", "type": "string"}
  ]
}
```

## Kafka Topics

The application creates and produces data to the following Kafka topics:

- `clicks`: Contains Click events
- `categories`: Contains Category data
- `enrichedClick`: Created but no data is produced to it (used by the processing applications)
