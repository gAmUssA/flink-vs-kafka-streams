# Real-Time Analytics System: Product Requirements Document

## 1. Introduction

### 1.1 Purpose
This document outlines the requirements for a real-time analytics system that calculates unique users per website category. The system will demonstrate the capabilities of modern stream processing frameworks and serve as a reference implementation for Java developers and streaming engineers.

### 1.2 Scope
The system will process user click events, join them with category information, and compute unique user counts per category over time windows. It will showcase three different implementation approaches using Kafka Streams and Apache Flink.

### 1.3 Definitions
- **Click Event**: A user interaction with a webpage, containing user ID, timestamp, and page ID
- **Category**: A classification of webpages (e.g., sports, news, entertainment)
- **Unique Users**: Distinct user IDs counted only once per category within a time window

## 2. Product Overview

### 2.1 Product Perspective
This system will be a standalone demo application that integrates with Apache Kafka for data ingestion and output. It will demonstrate real-world analytics capabilities using production-grade technologies.

### 2.2 Product Features
- Real-time processing of click events
- Joining of click events with category information
- Windowed aggregation of unique users per category
- Multiple implementation approaches for comparison

### 2.3 User Classes and Characteristics
- **Java Developers**: Looking to learn stream processing concepts and implementation patterns
- **Data Engineers**: Evaluating different stream processing frameworks for their projects
- **Solution Architects**: Comparing technologies for enterprise streaming applications

## 3. Functional Requirements

### 3.1 Data Processing Requirements
- FR1: Ingest click events from a Kafka topic named "clicks"
- FR2: Ingest category mappings from a Kafka topic named "categories"
- FR3: Join click events with their corresponding categories based on page_id
- FR4: Count unique users per category over a sliding one-hour window
- FR5: Update the counts every minute
- FR6: Output results to a Kafka topic named "output-topic"
- FR7: Provide access to intermediate state of processing

### 3.2 Data Model Requirements
- DR1: Click events must contain user_id (string), timestamp (long), and page_id (string)
- DR2: Category mappings must contain page_id (string) and category (string)
- DR3: Output records must contain category (string), unique_users (long), window_start, and window_end

## 4. Non-Functional Requirements

### 4.1 Performance Requirements
- PR1: Process events with low latency (under 5 seconds from ingestion to result)
- PR2: Handle at least 10,000 events per second
- PR3: Scale horizontally to accommodate increased load

### 4.2 Reliability Requirements
- RR1: Ensure exactly-once processing semantics
- RR2: Recover from failures without data loss
- RR3: Handle late-arriving data appropriately

### 4.3 Compatibility Requirements
- CR1: Use Apache Kafka 3.4.0 for messaging
- CR2: Use Apache Flink 1.20.0 for stream processing
- CR3: Use Avro for data serialization
- CR4: Use Confluent Schema Registry for schema management

## 5. Technical Requirements

### 5.1 Implementation Approaches
The system must be implemented using three different approaches:

#### 5.1.1 Kafka Streams Implementation
- TR1: Implement using Kafka Streams DSL in package `dev.gamov.streams.kafka`
- TR2: Use SpecificAvroSerde for Avro serialization/deserialization
- TR3: Configure Schema Registry integration
- TR4: Implement windowed aggregation for unique user counting

#### 5.1.2 Flink DataStream API Implementation
- TR5: Implement using Flink DataStream API in package `dev.gamov.streams.flink`
- TR6: Use ConfluentRegistryAvroDeserializationSchema for Avro deserialization
- TR7: Implement proper state management for joins
- TR8: Configure sliding windows for aggregation

#### 5.1.3 Flink Table API Implementation
- TR9: Implement using Flink Table API with builder-style in package `dev.gamov.streams.flink`
- TR10: Configure Kafka connectors with Avro format
- TR11: Use SQL-like operations for joins and aggregations
- TR12: Implement proper event time handling and watermarking

### 5.2 State Access Requirements
- SR1: Expose state stores containing unique users per category
- SR2: Provide query capabilities for state access
- SR3: Support querying by category and time window
- SR4: Implement a unified interface for state access across all implementations

## 6. Testing Requirements

### 6.1 Unit Testing
- UT1: Test individual components and helper methods
- UT2: Verify correct joining of clicks with categories
- UT3: Validate unique user counting logic

### 6.2 Integration Testing
- IT1: Test complete processing pipeline
- IT2: Verify end-to-end functionality with test data
- IT3: Use Testcontainers for integration testing with real Kafka and Schema Registry

## 7. Success Metrics

### 7.1 Technical Success Criteria
- Successful implementation of all three approaches
- All tests passing
- Meeting performance requirements
- Proper handling of edge cases

### 7.2 Business Success Criteria
- Demonstrating the capabilities of each framework
- Providing clear comparison points between implementations
- Creating a valuable reference for developers and engineers

## 8. References
- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- Apache Flink Documentation: https://flink.apache.org/
- Confluent Schema Registry: https://docs.confluent.io/platform/current/schema-registry/
- Flink Table API: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/
