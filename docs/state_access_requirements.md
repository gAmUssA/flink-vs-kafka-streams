# State Access Requirements for Flink vs Kafka Streams Project

In addition to computing unique users per category, the demo should provide capabilities to access the intermediate state of processing:

## 1. Kafka Streams State Access

- Expose the state store containing unique users per category
- Provide an interactive query service to access state stores
- Allow querying unique users for a specific category and time window
- Support both local and remote state store queries

## 2. Flink DataStream API State Access

- Implement queryable state for unique users per category
- Expose state through Flink's QueryableStateClient
- Provide a REST API endpoint to query the state
- Support querying state by category and time window

## 3. Flink Table API State Access

- Convert Table API operations to DataStream API where state can be accessed
- Create temporary views to query state using SQL
- Implement a custom UDTF (User-Defined Table Function) to expose state
- Provide a unified interface for state access across all implementations

These state access capabilities will demonstrate how to monitor and debug streaming applications in real-time, as well as how to build interactive applications that need to query the current state of the processing.