package dev.gamov.streams.flink;

import dev.gamov.streams.Category;
import dev.gamov.streams.Click;
import dev.gamov.streams.EnrichedClick;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example Flink Table API implementation for processing click data
 * This is a simplified pseudo-code version that demonstrates the concepts
 */
public class FlinkTableProcessor {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTableProcessor.class);

    public static void main(String[] args) throws Exception {
        logger.info("Flink Table API Example (Builder Style)");
        logger.info("---------------------------------------");
        logger.info("In a real implementation, we would:");
        logger.info("1. Set up a StreamExecutionEnvironment and TableEnvironment");
        logger.info("2. Define tables using TableSource with KafkaTableSourceSinkFactory");
        logger.info("3. Specify Avro-Confluent format and Schema Registry");
        logger.info("4. Join clicks and categories using Table API");
        logger.info("5. Group by category with a tumbling window");
        logger.info("6. Count distinct users using builder-style API");
        logger.info("7. Execute the Flink job");

        // Demonstrate with sample data
        Click click1 = createClick("user1", 1000L, "page1");
        Click click2 = createClick("user2", 2000L, "page2");
        Click click3 = createClick("user1", 3000L, "page3");

        Category category1 = createCategory("page1", "sports");
        Category category2 = createCategory("page2", "news");
        Category category3 = createCategory("page3", "entertainment");

        // Simulate joining clicks with categories
        EnrichedClick enrichedClick1 = joinClickWithCategory(click1, category1);
        EnrichedClick enrichedClick2 = joinClickWithCategory(click2, category2);
        EnrichedClick enrichedClick3 = joinClickWithCategory(click3, category3);

        List<EnrichedClick> enrichedClicks = Arrays.asList(
                enrichedClick1, enrichedClick2, enrichedClick3);

        // Simulate Table API operations
        logger.info("\nSimulated Table API Results:");
        logger.info("SQL equivalent: ");
        logger.info("SELECT category, COUNT(DISTINCT user_id) as unique_users");
        logger.info("FROM enriched_clicks");
        logger.info("GROUP BY category, TUMBLE(timestamp, INTERVAL '1' HOUR)");

        // Simulate results
        simulateTableResults(enrichedClicks);
    }

    /**
     * Helper method to create a Click object
     */
    private static Click createClick(String userId, long timestamp, String pageId) {
        logger.debug("Creating Click: userId={}, timestamp={}, pageId={}", userId, timestamp, pageId);
        Click click = new Click();
        click.setUserId(userId);
        click.setTimestamp(timestamp);
        click.setPageId(pageId);
        return click;
    }

    /**
     * Helper method to create a Category object
     */
    private static Category createCategory(String pageId, String category) {
        logger.debug("Creating Category: pageId={}, category={}", pageId, category);
        Category cat = new Category();
        cat.setPageId(pageId);
        cat.setCategory(category);
        return cat;
    }

    /**
     * Helper method to join a Click with a Category
     */
    private static EnrichedClick joinClickWithCategory(Click click, Category category) {
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

    /**
     * Helper method to simulate Table API results
     */
    private static void simulateTableResults(List<EnrichedClick> clicks) {
        logger.debug("Simulating Table API results for {} clicks", clicks.size());

        // Group by category and count unique users
        Set<String> sportsUsers = new HashSet<>();
        Set<String> newsUsers = new HashSet<>();
        Set<String> entertainmentUsers = new HashSet<>();

        for (EnrichedClick click : clicks) {
            String category = click.getCategory();
            String userId = click.getUserId();

            logger.trace("Processing click: userId={}, category={}", userId, category);

            if ("sports".equals(category)) {
                sportsUsers.add(userId);
                logger.trace("Added user {} to sports category", userId);
            } else if ("news".equals(category)) {
                newsUsers.add(userId);
                logger.trace("Added user {} to news category", userId);
            } else if ("entertainment".equals(category)) {
                entertainmentUsers.add(userId);
                logger.trace("Added user {} to entertainment category", userId);
            }
        }

        logger.info("\nCategory: sports, Unique Users: {}", sportsUsers.size());
        logger.info("Category: news, Unique Users: {}", newsUsers.size());
        logger.info("Category: entertainment, Unique Users: {}", entertainmentUsers.size());
    }
}