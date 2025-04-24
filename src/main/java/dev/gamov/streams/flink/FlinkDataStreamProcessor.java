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
 * Example Flink DataStream API implementation for processing click data
 * This is a simplified pseudo-code version that demonstrates the concepts
 */
public class FlinkDataStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(FlinkDataStreamProcessor.class);

    public static void main(String[] args) throws Exception {
        logger.info("Flink DataStream API Example");
        logger.info("----------------------------");
        logger.info("In a real implementation, we would:");
        logger.info("1. Set up a StreamExecutionEnvironment");
        logger.info("2. Configure Kafka sources with ConfluentRegistryAvroDeserializationSchema");
        logger.info("3. Create DataStreams from Kafka sources");
        logger.info("4. Join clicks with categories using KeyedCoProcessFunction");
        logger.info("5. Aggregate unique users per category in a sliding window");
        logger.info("6. Execute the Flink job");

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

        // Simulate aggregating unique users per category
        logger.info("\nSimulated Results:");
        int sportsUsers = countUniqueUsers(Arrays.asList(enrichedClick1), "sports");
        int newsUsers = countUniqueUsers(Arrays.asList(enrichedClick2), "news");
        int entertainmentUsers = countUniqueUsers(Arrays.asList(enrichedClick3), "entertainment");

        logger.info("Unique users in 'sports': {}", sportsUsers);
        logger.info("Unique users in 'news': {}", newsUsers);
        logger.info("Unique users in 'entertainment': {}", entertainmentUsers);
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
     * Helper method to count unique users for a category
     */
    private static int countUniqueUsers(List<EnrichedClick> clicks, String categoryName) {
        logger.debug("Counting unique users for category: {}", categoryName);
        Set<String> uniqueUsers = new HashSet<>();
        for (EnrichedClick click : clicks) {
            if (click.getCategory().equals(categoryName)) {
                logger.trace("Adding user {} to unique users set for category {}", 
                        click.getUserId(), categoryName);
                uniqueUsers.add(click.getUserId());
            }
        }
        logger.debug("Found {} unique users for category {}", uniqueUsers.size(), categoryName);
        return uniqueUsers.size();
    }
}