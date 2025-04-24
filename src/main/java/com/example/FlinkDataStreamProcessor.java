package com.example;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Example Flink DataStream API implementation for processing click data
 * This is a simplified pseudo-code version that demonstrates the concepts
 */
public class FlinkDataStreamProcessor {

    public static void main(String[] args) throws Exception {
        System.out.println("Flink DataStream API Example");
        System.out.println("----------------------------");
        System.out.println("In a real implementation, we would:");
        System.out.println("1. Set up a StreamExecutionEnvironment");
        System.out.println("2. Configure Kafka sources with ConfluentRegistryAvroDeserializationSchema");
        System.out.println("3. Create DataStreams from Kafka sources");
        System.out.println("4. Join clicks with categories using KeyedCoProcessFunction");
        System.out.println("5. Aggregate unique users per category in a sliding window");
        System.out.println("6. Execute the Flink job");
        
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
        System.out.println("\nSimulated Results:");
        System.out.println("Unique users in 'sports': " + 
                countUniqueUsers(Arrays.asList(enrichedClick1), "sports"));
        System.out.println("Unique users in 'news': " + 
                countUniqueUsers(Arrays.asList(enrichedClick2), "news"));
        System.out.println("Unique users in 'entertainment': " + 
                countUniqueUsers(Arrays.asList(enrichedClick3), "entertainment"));
    }
    
    /**
     * Helper method to create a Click object
     */
    private static Click createClick(String userId, long timestamp, String pageId) {
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
        Category cat = new Category();
        cat.setPageId(pageId);
        cat.setCategory(category);
        return cat;
    }
    
    /**
     * Helper method to join a Click with a Category
     */
    private static EnrichedClick joinClickWithCategory(Click click, Category category) {
        EnrichedClick enrichedClick = new EnrichedClick();
        enrichedClick.setUserId(click.getUserId());
        enrichedClick.setTimestamp(click.getTimestamp());
        enrichedClick.setCategory(category.getCategory());
        return enrichedClick;
    }
    
    /**
     * Helper method to count unique users for a category
     */
    private static int countUniqueUsers(List<EnrichedClick> clicks, String categoryName) {
        Set<String> uniqueUsers = new HashSet<>();
        for (EnrichedClick click : clicks) {
            if (click.getCategory().equals(categoryName)) {
                uniqueUsers.add(click.getUserId());
            }
        }
        return uniqueUsers.size();
    }
}