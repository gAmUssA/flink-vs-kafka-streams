package com.example;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for FlinkDataStreamProcessor
 */
public class FlinkDataStreamProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(FlinkDataStreamProcessorTest.class);

    /**
     * Test that demonstrates how to test the joinClickWithCategory method
     */
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

        // Call the method under test using reflection (since it's private)
        java.lang.reflect.Method joinMethod = FlinkDataStreamProcessor.class.getDeclaredMethod(
                "joinClickWithCategory", Click.class, Category.class);
        joinMethod.setAccessible(true);
        EnrichedClick result = (EnrichedClick) joinMethod.invoke(null, click, category);

        // Verify the result
        assertNotNull(result, "Joined result should not be null");
        assertEquals("testUser", result.getUserId(), "User ID should match");
        assertEquals(1000L, result.getTimestamp(), "Timestamp should match");
        assertEquals("testCategory", result.getCategory(), "Category should match");

        logger.debug("Test passed: joinClickWithCategory correctly joined Click and Category");
    }
}
