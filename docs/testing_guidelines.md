# Testing Guidelines for Flink vs Kafka Streams Project

This document provides guidelines and best practices for testing in the Flink vs Kafka Streams comparison project.

## Table of Contents
1. [Unit Testing](#unit-testing)
2. [Integration Testing](#integration-testing)
3. [Test Data Generation](#test-data-generation)
4. [Using TestDataProducer Utility](#using-testdataproducer-utility)
5. [Best Practices](#best-practices)

## Unit Testing

Unit tests should focus on testing individual components and methods in isolation. The project uses JUnit 5 for unit testing.

### Guidelines for Unit Tests
- Test one thing per test method
- Use descriptive test method names
- Use assertions to verify expected outcomes
- Mock external dependencies when necessary
- Test edge cases and error conditions

## Integration Testing

Integration tests verify that different components work together correctly. The project uses Testcontainers to set up real Kafka and Schema Registry instances for integration testing.

### Guidelines for Integration Tests
- Extend `KafkaTCIntegrationTestBase` to leverage common setup code
- Use the `@Timeout` annotation to prevent tests from hanging indefinitely
- Use Awaitility for asynchronous testing instead of arbitrary Thread.sleep calls
- Use AssertJ for more readable assertions

## Test Data Generation

The project includes a utility class `TestDataProducer` for creating test data and producing it to Kafka. This utility encapsulates the common functionality used across integration tests.

### TestDataProducer Features
- Creates standard test categories and clicks
- Produces data to Kafka topics
- Configures Avro serialization with Schema Registry
- Provides a simple API for test data generation

## Using TestDataProducer Utility

The `TestDataProducer` utility class simplifies the creation and production of test data to Kafka. Here's how to use it in your integration tests:

```java
// Create a TestDataProducer instance
TestDataProducer testDataProducer = new TestDataProducer(
    kafkaContainer.getBootstrapServers(),
    schemaRegistryContainer
);

// Produce standard test data (categories and clicks)
testDataProducer.produceStandardTestData();

// Or create and produce custom test data
List<Category> customCategories = testDataProducer.createTestCategories();
// Modify categories as needed
testDataProducer.produceCategories(customCategories);

List<Click> customClicks = testDataProducer.createTestClicks();
// Modify clicks as needed
testDataProducer.produceClicks(customClicks);
```

### Standard Test Data
The standard test data includes:

**Categories:**
- sports (page1)
- news (page2)
- entertainment (page3)

**Clicks:**
- User1 clicks on page1 (sports)
- User2 clicks on page1 (sports)
- User1 clicks on page2 (news)
- User3 clicks on page2 (news)
- User4 clicks on page3 (entertainment)

This standard test data is designed to test various scenarios:
- Multiple users clicking on the same page (sports, news)
- Same user clicking on different pages (User1)
- Single user clicking on a page (entertainment)

## Best Practices

### For Integration Tests
1. **Use TestDataProducer**: Instead of creating test data manually in each test, use the TestDataProducer utility.
2. **Avoid Duplication**: Don't duplicate code for creating and producing test data.
3. **Consistent Test Data**: Use standard test data when possible for consistency across tests.
4. **Custom Test Data**: When standard test data isn't sufficient, create custom test data based on your specific test requirements.
5. **Clean Up**: Always clean up resources after tests, especially when using external systems like Kafka.

### For Adding New Tests
1. Follow the existing patterns in the codebase
2. Use the TestDataProducer utility for test data generation
3. Add appropriate assertions to verify expected behavior
4. Document any special test setup or requirements
