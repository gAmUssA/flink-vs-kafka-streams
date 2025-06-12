package dev.gamov.streams.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Flink Table API implementation for processing click data
 * This implementation reads clicks and categories from Kafka topics,
 * joins them, and counts unique users per category in a sliding window.
 */
public class FlinkTableApiProcessor {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTableApiProcessor.class);

    // Topic names as constants for easier testing
    public static final String CLICKS_TOPIC = "clicks";
    public static final String CATEGORIES_TOPIC = "categories";
    public static final String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) throws Exception {
        logger.info("Starting Flink Table API Processor application");

        // Configure properties
        Properties properties = createProperties();

        // Set up StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Configure tables and process data
        processData(tableEnv, properties);

        // Execute the Flink job
        env.execute("Flink Table API Processor");
    }

    /**
     * Creates properties for Kafka and Schema Registry
     */
    public static Properties createProperties() {
        logger.debug("Creating properties for Kafka and Schema Registry");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        logger.debug("Properties created: {}", properties);
        return properties;
    }

    /**
     * Process the data using Table API
     * 1. Define clicks and categories tables
     * 2. Join tables
     * 3. Group by category with a sliding window
     * 4. Count distinct users
     * 5. Output results to Kafka
     */
    public static void processData(StreamTableEnvironment tableEnv, Properties properties) {
        logger.info("Processing data using Table API");

        String bootstrapServers = properties.getProperty("bootstrap.servers");
        String schemaRegistryUrl = properties.getProperty("schema.registry.url");

        // 1. Define clicks table
        defineClicksTable(tableEnv, bootstrapServers, schemaRegistryUrl);
        logger.debug("Clicks table defined successfully");

        // 2. Define categories table
        defineCategoriesTable(tableEnv, bootstrapServers, schemaRegistryUrl);
        logger.debug("Categories table defined successfully");

        // 3. Join tables, group by category with sliding window, and count distinct users
        Table resultTable = joinAndAggregate(tableEnv);
        logger.debug("Tables joined and aggregated successfully");

        // 4. Convert result table to DataStream
        DataStream<Tuple2<String, String>> resultStream = tableToDataStream(tableEnv, resultTable);
        logger.debug("Result table converted to DataStream");

        // 5. Output results to Kafka and print for debugging
        outputResults(resultStream, bootstrapServers);
        logger.info("Results sent to Kafka topic: {}", OUTPUT_TOPIC);
    }

    /**
     * Define clicks table using TableDescriptor
     */
    private static void defineClicksTable(
            StreamTableEnvironment tableEnv, 
            String bootstrapServers, 
            String schemaRegistryUrl) {

        logger.debug("Defining clicks table from topic: {}", CLICKS_TOPIC);

        // Create the base clicks table with just the columns from the Avro schema
        tableEnv.createTable("clicks_raw", TableDescriptor.forConnector("kafka")
            .schema(Schema.newBuilder()
                .column("user_id", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .column("page_id", DataTypes.STRING())
                .build())
            .option("topic", CLICKS_TOPIC)
            .option("properties.bootstrap.servers", bootstrapServers)
            .option("properties.group.id", "flink-table-clicks-consumer")
            .option("scan.startup.mode", "earliest-offset")
            .format("avro-confluent")
            .option("avro-confluent.schema-registry.url", schemaRegistryUrl)
            .build());

        // Create a view with processing time attribute for the window aggregation
        tableEnv.executeSql(
            "CREATE VIEW clicks_with_proc_time AS " +
            "SELECT user_id, `timestamp`, page_id, PROCTIME() AS proc_time " +
            "FROM clicks_raw"
        );

        // Create a view with processing time attribute for the window aggregation
        tableEnv.executeSql(
            "CREATE VIEW clicks_with_proc_time AS " +
            "SELECT user_id, `timestamp`, page_id, PROCTIME() AS proc_time " +
            "FROM clicks_raw"
        );

        logger.debug("Created clicks view with event_time column");

        logger.debug("Clicks table and view defined successfully");
    }

    /**
     * Define categories table using TableDescriptor
     */
    private static void defineCategoriesTable(
            StreamTableEnvironment tableEnv, 
            String bootstrapServers, 
            String schemaRegistryUrl) {

        logger.debug("Defining categories table from topic: {}", CATEGORIES_TOPIC);

        tableEnv.createTable("categories", TableDescriptor.forConnector("kafka")
            .schema(Schema.newBuilder()
                .column("page_id", DataTypes.STRING())
                .column("category", DataTypes.STRING())
                .build())
            .option("topic", CATEGORIES_TOPIC)
            .option("properties.bootstrap.servers", bootstrapServers)
            .option("properties.group.id", "flink-table-categories-consumer")
            .option("scan.startup.mode", "earliest-offset")
            .format("avro-confluent")
            .option("avro-confluent.schema-registry.url", schemaRegistryUrl)
            .build());
    }

    /**
     * Join tables, group by category with sliding window, and count distinct users
     */
    private static Table joinAndAggregate(StreamTableEnvironment tableEnv) {
        logger.debug("Joining tables and aggregating data");

        // Join clicks and categories tables
        // Note: timestamp is a reserved keyword, so we need to escape it with backticks
        Table joinedTable = tableEnv.sqlQuery(
            "SELECT c.user_id, c.`timestamp`, c.proc_time, cat.category " +
            "FROM clicks_with_proc_time AS c " +
            "JOIN categories AS cat ON c.page_id = cat.page_id"
        );

        // Register the joined table as a temporary view
        tableEnv.createTemporaryView("joined_clicks", joinedTable);

        // Group by category with tumbling window and count distinct users
        return tableEnv.sqlQuery(
            "SELECT " +
            "  category, " +
            "  TUMBLE_START(proc_time, INTERVAL '1' HOUR) AS window_start, " +
            "  TUMBLE_END(proc_time, INTERVAL '1' HOUR) AS window_end, " +
            "  COUNT(DISTINCT user_id) AS unique_users " +
            "FROM joined_clicks " +
            "GROUP BY TUMBLE(proc_time, INTERVAL '1' HOUR), category"
        );
    }

    /**
     * Convert result table to DataStream
     */
    private static DataStream<Tuple2<String, String>> tableToDataStream(
            StreamTableEnvironment tableEnv, Table resultTable) {

        logger.debug("Converting result table to DataStream");

        // Convert Table to DataStream
        return tableEnv.toDataStream(resultTable)
            .map(new MapFunction<org.apache.flink.types.Row, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(org.apache.flink.types.Row row) throws Exception {
                    String category = row.getFieldAs("category");
                    Long uniqueUsers = row.getFieldAs("unique_users");
                    String windowStart = row.getFieldAs("window_start").toString();
                    String windowEnd = row.getFieldAs("window_end").toString();

                    String resultValue = String.format(
                        "Window[%s - %s]: %d unique users", 
                        windowStart, windowEnd, uniqueUsers);

                    logger.debug("Mapped result: Category={}, {}", category, resultValue);
                    return new Tuple2<>(category, resultValue);
                }
            });
    }

    /**
     * Output results to Kafka and print for debugging
     */
    private static void outputResults(
            DataStream<Tuple2<String, String>> resultStream, 
            String bootstrapServers) {

        logger.debug("Sending results to Kafka and printing for debugging");

        // Create KafkaSink for sending results to Kafka
        org.apache.flink.connector.kafka.sink.KafkaSink<Tuple2<String, String>> kafkaSink = 
            org.apache.flink.connector.kafka.sink.KafkaSink.<Tuple2<String, String>>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                    org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
                        .<Tuple2<String, String>>builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setKeySerializationSchema(
                            (Tuple2<String, String> element) -> element.f0.getBytes())
                        .setValueSerializationSchema(
                            (Tuple2<String, String> element) -> element.f1.getBytes())
                        .build()
                )
                .setDeliveryGuarantee(
                    org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Send results to Kafka using KafkaSink
        resultStream.sinkTo(kafkaSink);

        // Also print the results for debugging
        resultStream.map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> tuple) throws Exception {
                String category = tuple.f0;
                String result = tuple.f1;

                logger.debug("Processing result - Category: {}, Result: {}", category, result);
                return "Category: " + category + ", " + result;
            }
        }).print();
    }
}
