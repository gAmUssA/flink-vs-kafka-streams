package dev.gamov.streams.flink;

import dev.gamov.streams.Category;
import dev.gamov.streams.Click;
import dev.gamov.streams.EnrichedClick;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test class for FlinkDataStreamProcessor
 */
public class FlinkDataStreamProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(FlinkDataStreamProcessorTest.class);

    @Test
    public void testJoinClickWithCategory() throws Exception {
        // Create test data
        Click click = new Click();
        click.setUserId("testUser");
        click.setTimestamp(1000L);
        click.setPageId("testPage");

        Category category = new Category();
        category.setPageId("testPage");
        category.setCategory("testCategory");

        // Use reflection to access the private method
        java.lang.reflect.Method joinMethod = FlinkDataStreamProcessor.class.getDeclaredMethod(
                "joinClickWithCategory", Click.class, Category.class);
        joinMethod.setAccessible(true);

        // Invoke the method
        EnrichedClick result = (EnrichedClick) joinMethod.invoke(null, click, category);

        // Verify the result
        assertNotNull(result, "Joined result should not be null");
        assertEquals("testUser", result.getUserId(), "User ID should match");
        assertEquals(1000L, result.getTimestamp(), "Timestamp should match");
        assertEquals("testCategory", result.getCategory(), "Category should match");

        logger.debug("Test passed: joinClickWithCategory correctly joined Click and Category");
    }

    @Test
    public void testCountUniqueUsers() throws Exception {
        // Create test data
        EnrichedClick click1 = new EnrichedClick();
        click1.setUserId("user1");
        click1.setTimestamp(1000L);
        click1.setCategory("sports");

        EnrichedClick click2 = new EnrichedClick();
        click2.setUserId("user2");
        click2.setTimestamp(2000L);
        click2.setCategory("sports");

        EnrichedClick click3 = new EnrichedClick();
        click3.setUserId("user1");
        click3.setTimestamp(3000L);
        click3.setCategory("sports");

        List<EnrichedClick> clicks = Arrays.asList(click1, click2, click3);

        // Use reflection to access the private method
        java.lang.reflect.Method countMethod = FlinkDataStreamProcessor.class.getDeclaredMethod(
                "countUniqueUsers", List.class, String.class);
        countMethod.setAccessible(true);

        // Invoke the method
        int result = (int) countMethod.invoke(null, clicks, "sports");

        // Verify the result
        assertEquals(2, result, "Should count 2 unique users");

        logger.debug("Test passed: countUniqueUsers correctly counted unique users");
    }
}