# Implementation Plan for Flink vs Kafka Streams Project

## Project Overview
This document outlines the implementation plan for the real-time analytics demo that calculates unique users per website category using Apache Kafka, Apache Flink, Avro serialization, and Confluent Schema Registry. The demo will showcase three different implementations:

1. Kafka Streams implementation
2. Flink DataStream API implementation
3. Flink Table API implementation (using builder-style)

## Current Status

### What's Already Implemented
- Project structure and build configuration with Gradle
- Avro schemas for Click, Category, and EnrichedClick
- Basic Kafka Streams implementation (needs updates)
- Placeholder implementations for Flink DataStream and Table API
- Basic test cases

### What Needs to be Implemented
- Update Kafka Streams implementation to count unique users
- Complete Flink DataStream API implementation
- Complete Flink Table API implementation with builder-style
- Comprehensive test coverage for all implementations

## Version Discrepancy
There is a discrepancy between the versions specified in the requirements and the versions in the build.gradle.kts file:

| Component | Requirements | Current Build |
|-----------|--------------|--------------|
| Kafka     | 4.0.0        | 3.4.0        |
| Flink     | 2.0.0        | 1.20.0       |

Since the requirements document specifies using Kafka 4.0.0 and Flink 2.0.0, but these versions don't appear to be available yet (as of the current implementation), we'll proceed with the versions in the build.gradle.kts file (Kafka 3.4.0 and Flink 1.20.0) but ensure our implementation is forward-compatible with the specified versions when they become available.

## Implementation Tasks

### 1. Kafka Streams Implementation

#### Current Status
- Basic implementation exists
- Uses simple count aggregation instead of counting unique users

#### Tasks
1. Update the aggregation logic to count unique users per category
   - Use a HashSet to track unique user IDs
   - Modify the aggregate function to add user IDs to the set
   - Map the result to count the size of the set
2. Update the windowing to use a sliding one-hour window updated every minute
3. Ensure proper serialization/deserialization of the HashSet
4. Update tests to verify unique user counting
5. Implement state store access
   - Configure state store with a meaningful name
   - Implement an interactive query service
   - Create methods to query unique users by category and time window
   - Support both local and remote state store queries
6. Create a REST API for state access
   - Implement endpoints to query state stores
   - Add documentation for API usage

### 2. Flink DataStream API Implementation

#### Current Status
- Only a placeholder implementation exists
- Basic helper methods are implemented

#### Tasks
1. Implement a complete Flink DataStream application:
   - Set up StreamExecutionEnvironment
   - Configure Kafka sources with ConfluentRegistryAvroDeserializationSchema
   - Create DataStreams from Kafka sources
   - Join clicks with categories using KeyedCoProcessFunction
   - Implement a sliding window (1 hour, updated every minute)
   - Aggregate unique users per category
   - Output results to Kafka
2. Implement proper state management for the join operation
3. Implement proper error handling and logging
4. Create comprehensive tests using Flink's testing utilities
5. Implement queryable state:
   - Configure state descriptors to be queryable
   - Register state with meaningful names
   - Implement QueryableStateClient for external access
   - Create methods to query state by category and time window
6. Create a REST API for state access:
   - Implement a REST server to expose state
   - Create endpoints to query state
   - Add documentation for API usage

### 3. Flink Table API Implementation (Builder Style)

#### Current Status
- Only a placeholder implementation exists
- Basic helper methods are implemented

#### Tasks
1. Implement a complete Flink Table API application using builder-style:
   - Set up StreamExecutionEnvironment and TableEnvironment
   - Define tables using TableSource with KafkaTableSourceSinkFactory
   - Specify Avro-Confluent format and Schema Registry
   - Join clicks and categories using Table API
   - Group by category with a sliding window (1 hour, updated every minute)
   - Count distinct users using builder-style API
   - Output results to Kafka
2. Ensure proper event time handling and watermarking
3. Implement proper error handling and logging
4. Create comprehensive tests for the Table API implementation
5. Implement state access for Table API:
   - Create a bridge between Table API and DataStream API for state access
   - Implement temporary views to query state using SQL
   - Create a custom UDTF (User-Defined Table Function) to expose state
   - Develop methods to query state by category and time window
6. Create a unified interface for state access:
   - Implement a common API for accessing state across all implementations
   - Create documentation for the unified state access interface

## Testing Strategy

### Unit Tests
- Test individual components and helper methods
- Use reflection for testing private methods when necessary

### Integration Tests
- Test the complete processing pipeline
- For Kafka Streams, use TopologyTestDriver
- For Flink, use MiniClusterWithClientResource or similar testing utilities

### Integration Tests with Testcontainers
- Use Testcontainers to create real Kafka, Zookeeper, and Schema Registry containers for testing
- Test the complete pipeline with actual Kafka and Schema Registry instances
- Verify end-to-end functionality with real infrastructure
- Ensure compatibility with production environment
- Test schema evolution and serialization/deserialization with actual Schema Registry

### Test Data
- Create test data that covers various scenarios:
  - Multiple users visiting the same category
  - Same user visiting multiple categories
  - Edge cases like missing categories or late events

## Implementation Timeline

### Phase 1: Update Kafka Streams Implementation
- Update aggregation logic to count unique users
- Update windowing configuration
- Update tests
- Estimated time: 1-2 days

### Phase 2: Implement Flink DataStream API
- Implement complete DataStream application
- Implement state management and error handling
- Create tests
- Estimated time: 2-3 days

### Phase 3: Implement Flink Table API (Builder Style)
- Implement complete Table API application
- Implement event time handling and error handling
- Create tests
- Estimated time: 2-3 days

### Phase 4: Implement State Access
- Implement Kafka Streams state store access and REST API
- Implement Flink DataStream queryable state and REST API
- Implement Flink Table API state access and bridge to DataStream API
- Create unified interface for state access
- Estimated time: 3-4 days

### Phase 5: Documentation and Final Testing
- Update documentation
- Perform final testing
- Create comparison metrics
- Estimated time: 1-2 days

## Conclusion
This implementation plan outlines the steps needed to complete the real-time analytics demo using Kafka Streams and Flink. By following this plan, we will create a comprehensive comparison of the different approaches to stream processing, showcasing the strengths and features of each framework.
