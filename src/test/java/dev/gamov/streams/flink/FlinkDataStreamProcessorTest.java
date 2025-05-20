package dev.gamov.streams.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import dev.gamov.streams.Category;
import dev.gamov.streams.Click;
import dev.gamov.streams.EnrichedClick;
import dev.gamov.streams.flink.FlinkDataStreamProcessor.ClickCategoryJoinFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void testClickCategoryJoinFunction() throws Exception {
        logger.info("Testing ClickCategoryJoinFunction");

        // Create the join function
        ClickCategoryJoinFunction joinFunction = new ClickCategoryJoinFunction();

        // Create a test harness for the join function
        KeyedTwoInputStreamOperatorTestHarness<String, Click, Category, EnrichedClick> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new KeyedCoProcessOperator<>(joinFunction),
                        click -> click.getPageId(),
                        category -> category.getPageId(),
                        TypeInformation.of(String.class));

        // Initialize the test harness
        testHarness.open();

        // Create test data
        Click click1 = new Click();
        click1.setUserId("user1");
        click1.setTimestamp(1000L);
        click1.setPageId("page1");

        Click click2 = new Click();
        click2.setUserId("user2");
        click2.setTimestamp(2000L);
        click2.setPageId("page2");

        Click clickWithEmptyPageId = new Click();
        clickWithEmptyPageId.setUserId("user3");
        clickWithEmptyPageId.setTimestamp(3000L);
        clickWithEmptyPageId.setPageId("");

        Category category1 = new Category();
        category1.setPageId("page1");
        category1.setCategory("sports");

        Category category2 = new Category();
        category2.setPageId("page2");
        category2.setCategory("news");

        // Process categories first
        testHarness.processElement2(new StreamRecord<>(category1, 0));
        testHarness.processElement2(new StreamRecord<>(category2, 0));

        // Then process clicks
        testHarness.processElement1(new StreamRecord<>(click1, 0));
        testHarness.processElement1(new StreamRecord<>(click2, 0));
        testHarness.processElement1(new StreamRecord<>(clickWithEmptyPageId, 0));

        // Get the collected output
        List<EnrichedClick> output = new ArrayList<>();
        for (Object recordObj : testHarness.extractOutputStreamRecords()) {
            @SuppressWarnings("unchecked")
            StreamRecord<EnrichedClick> record = (StreamRecord<EnrichedClick>) recordObj;
            output.add(record.getValue());
        }

        // Verify the output
        assertEquals(2, output.size(), "Should have 2 enriched clicks");

        EnrichedClick enrichedClick1 = output.get(0);
        assertEquals("user1", enrichedClick1.getUserId(), "User ID should match");
        assertEquals(1000L, enrichedClick1.getTimestamp(), "Timestamp should match");
        assertEquals("sports", enrichedClick1.getCategory(), "Category should match");

        EnrichedClick enrichedClick2 = output.get(1);
        assertEquals("user2", enrichedClick2.getUserId(), "User ID should match");
        assertEquals(2000L, enrichedClick2.getTimestamp(), "Timestamp should match");
        assertEquals("news", enrichedClick2.getCategory(), "Category should match");

        // Clean up
        testHarness.close();

        logger.info("ClickCategoryJoinFunction test passed");
    }

    @Test
    public void testUniqueUsersAggregateFunction() {
        logger.info("Testing UniqueUsersAggregateFunction");

        // Create the aggregate function
        UniqueUsersAggregateFunction aggregateFunction = new UniqueUsersAggregateFunction();

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

        // Test createAccumulator
        Tuple2<String, HashSet<String>> accumulator = aggregateFunction.createAccumulator();
        assertTrue(accumulator.f1.isEmpty(), "Initial accumulator should be empty");
        assertEquals("", accumulator.f0, "Initial category should be empty string");

        // Test add
        accumulator = aggregateFunction.add(click1, accumulator);
        assertEquals(1, accumulator.f1.size(), "Accumulator should have 1 user");
        assertTrue(accumulator.f1.contains("user1"), "Accumulator should contain user1");
        assertEquals("sports", accumulator.f0, "Category should be 'sports'");

        accumulator = aggregateFunction.add(click2, accumulator);
        assertEquals(2, accumulator.f1.size(), "Accumulator should have 2 users");
        assertTrue(accumulator.f1.contains("user1"), "Accumulator should contain user1");
        assertTrue(accumulator.f1.contains("user2"), "Accumulator should contain user2");

        accumulator = aggregateFunction.add(click3, accumulator);
        assertEquals(2, accumulator.f1.size(), "Accumulator should still have 2 users (no duplicates)");

        // Test getResult
        Tuple2<String, String> result = aggregateFunction.getResult(accumulator);
        assertEquals("sports", result.f0, "Category should be 'sports'");
        assertEquals("Count: 2", result.f1, "Count should be 2");

        // Test merge
        Tuple2<String, HashSet<String>> accumulator2 = new Tuple2<>("sports", new HashSet<>());
        accumulator2.f1.add("user3");
        accumulator2.f1.add("user4");

        Tuple2<String, HashSet<String>> mergedAccumulator = aggregateFunction.merge(accumulator, accumulator2);
        assertEquals(4, mergedAccumulator.f1.size(), "Merged accumulator should have 4 users");
        assertTrue(mergedAccumulator.f1.contains("user1"), "Merged accumulator should contain user1");
        assertTrue(mergedAccumulator.f1.contains("user2"), "Merged accumulator should contain user2");
        assertTrue(mergedAccumulator.f1.contains("user3"), "Merged accumulator should contain user3");
        assertTrue(mergedAccumulator.f1.contains("user4"), "Merged accumulator should contain user4");
        assertEquals("sports", mergedAccumulator.f0, "Category should be 'sports'");

        logger.info("UniqueUsersAggregateFunction test passed");
    }
}
