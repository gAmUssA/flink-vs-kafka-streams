# Flink vs Kafka Streams Project Task List

This document contains a detailed enumerated task list for implementing the real-time analytics demo that calculates unique users per website category using Apache Kafka, Apache Flink, Avro serialization, and Confluent Schema Registry.

## 1. Kafka Streams Implementation

### 1.1. Update Aggregation Logic
- [x] 1.1.1. Implement HashSet to track unique user IDs
- [x] 1.1.2. Modify the aggregate function to add user IDs to the set
- [x] 1.1.3. Map the result to count the size of the set

### 1.2. Update Windowing Configuration
- [x] 1.2.1. Implement sliding one-hour window
- [x] 1.2.2. Configure window to update every minute

### 1.3. Serialization/Deserialization
- [x] 1.3.1. Implement proper serialization of HashSet
- [x] 1.3.2. Implement proper deserialization of HashSet

### 1.4. Testing
- [x] 1.4.1. Update existing tests to verify unique user counting
- [x] 1.4.2. Add new tests for edge cases
- [x] 1.4.3. Verify window functionality in tests

### 1.5. Integration Testing with Testcontainers
- [x] 1.5.1. Add Testcontainers dependencies to the project
- [x] 1.5.2. Implement Kafka and Schema Registry containers
- [x] 1.5.3. Create integration tests for Kafka Streams with real Kafka
- [x] 1.5.4. Test schema evolution and serialization with actual Schema Registry
- [x] 1.5.5. Verify end-to-end functionality with real infrastructure

## 2. Flink DataStream API Implementation

### 2.1. Basic Setup
- [ ] 2.1.1. Set up StreamExecutionEnvironment
- [ ] 2.1.2. Configure Kafka sources with ConfluentRegistryAvroDeserializationSchema
- [ ] 2.1.3. Create DataStreams from Kafka sources

### 2.2. Data Processing
- [ ] 2.2.1. Join clicks with categories using KeyedCoProcessFunction
- [ ] 2.2.2. Implement sliding window (1 hour, updated every minute)
- [ ] 2.2.3. Aggregate unique users per category
- [ ] 2.2.4. Output results to Kafka

### 2.3. State Management and Error Handling
- [ ] 2.3.1. Implement proper state management for the join operation
- [ ] 2.3.2. Add error handling for missing data
- [ ] 2.3.3. Implement logging for debugging and monitoring

### 2.4. Testing
- [ ] 2.4.1. Create unit tests for helper methods
- [ ] 2.4.2. Implement integration tests using Flink's testing utilities
- [ ] 2.4.3. Test with various data scenarios

### 2.5. Integration Testing with Testcontainers
- [ ] 2.5.1. Reuse Testcontainers setup from Kafka Streams implementation
- [ ] 2.5.2. Create integration tests for Flink DataStream with real Kafka
- [ ] 2.5.3. Test Flink checkpointing with actual infrastructure
- [ ] 2.5.4. Verify Avro serialization/deserialization with Schema Registry
- [ ] 2.5.5. Test end-to-end pipeline with real-time data processing

## 3. Flink Table API Implementation (Builder Style)

### 3.1. Environment Setup
- [ ] 3.1.1. Set up StreamExecutionEnvironment
- [ ] 3.1.2. Set up TableEnvironment

### 3.2. Table Definitions
- [ ] 3.2.1. Define clicks table using TableSource with KafkaTableSourceSinkFactory
- [ ] 3.2.2. Define categories table using TableSource with KafkaTableSourceSinkFactory
- [ ] 3.2.3. Specify Avro-Confluent format and Schema Registry for both tables

### 3.3. Table Operations
- [ ] 3.3.1. Join clicks and categories using Table API
- [ ] 3.3.2. Group by category with a sliding window (1 hour, updated every minute)
- [ ] 3.3.3. Count distinct users using builder-style API
- [ ] 3.3.4. Output results to Kafka

### 3.4. Event Time and Error Handling
- [ ] 3.4.1. Implement proper event time handling
- [ ] 3.4.2. Configure watermarking for late events
- [ ] 3.4.3. Add error handling and logging

### 3.5. Testing
- [ ] 3.5.1. Create unit tests for Table API operations
- [ ] 3.5.2. Implement integration tests
- [ ] 3.5.3. Verify window and aggregation functionality

### 3.6. Integration Testing with Testcontainers
- [ ] 3.6.1. Reuse Testcontainers setup from previous implementations
- [ ] 3.6.2. Create integration tests for Flink Table API with real Kafka
- [ ] 3.6.3. Test SQL queries and table operations with actual data
- [ ] 3.6.4. Verify Avro format configuration with Schema Registry
- [ ] 3.6.5. Test end-to-end pipeline with builder-style API

## 4. State Access Implementation

### 4.1. Kafka Streams State Access
- [ ] 4.1.1. Configure state store with a meaningful name
- [ ] 4.1.2. Implement an interactive query service
- [ ] 4.1.3. Create methods to query unique users by category
- [ ] 4.1.4. Add support for querying by time window
- [ ] 4.1.5. Implement support for remote state store queries

### 4.2. Kafka Streams REST API
- [ ] 4.2.1. Create a REST server for state access
- [ ] 4.2.2. Implement endpoints to query state stores
- [ ] 4.2.3. Add error handling for REST API
- [ ] 4.2.4. Create documentation for API usage

### 4.3. Flink DataStream State Access
- [ ] 4.3.1. Configure state descriptors to be queryable
- [ ] 4.3.2. Register state with meaningful names
- [ ] 4.3.3. Implement QueryableStateClient for external access
- [ ] 4.3.4. Create methods to query state by category
- [ ] 4.3.5. Add support for querying by time window

### 4.4. Flink DataStream REST API
- [ ] 4.4.1. Implement a REST server to expose state
- [ ] 4.4.2. Create endpoints to query state
- [ ] 4.4.3. Add error handling for REST API
- [ ] 4.4.4. Create documentation for API usage

### 4.5. Flink Table API State Access
- [ ] 4.5.1. Create a bridge between Table API and DataStream API
- [ ] 4.5.2. Implement temporary views to query state using SQL
- [ ] 4.5.3. Create a custom UDTF to expose state
- [ ] 4.5.4. Develop methods to query state by category and time window

### 4.6. Unified State Access Interface
- [ ] 4.6.1. Define a common interface for state access
- [ ] 4.6.2. Implement adapters for each framework
- [ ] 4.6.3. Create a unified REST API for all implementations
- [ ] 4.6.4. Document the unified state access interface

## 5. Documentation and Final Testing

### 5.1. Documentation
- [ ] 5.1.1. Update README with setup instructions
- [ ] 5.1.2. Document Kafka Streams implementation details
- [ ] 5.1.3. Document Flink DataStream API implementation details
- [ ] 5.1.4. Document Flink Table API implementation details
- [ ] 5.1.5. Document state access implementations
- [ ] 5.1.6. Add comments to code for better readability

### 5.2. Final Testing
- [ ] 5.2.1. Run all tests to ensure everything works
- [ ] 5.2.2. Test with larger datasets
- [ ] 5.2.3. Verify performance under load
- [ ] 5.2.4. Test state access functionality

### 5.3. Comparison Metrics
- [ ] 5.3.1. Create comparison metrics between implementations
- [ ] 5.3.2. Document strengths and weaknesses of each approach
- [ ] 5.3.3. Compare state access capabilities across frameworks
- [ ] 5.3.4. Provide recommendations for different use cases
