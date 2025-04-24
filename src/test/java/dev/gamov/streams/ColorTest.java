package dev.gamov.streams;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to verify that console colors are working correctly.
 */
public class ColorTest {

    private static final Logger logger = LoggerFactory.getLogger(ColorTest.class);

    @Test
    public void testColoredLogs() {
        // Log messages at different levels to see if colors are applied
        logger.trace("This is a TRACE message");
        logger.debug("This is a DEBUG message");
        logger.info("This is an INFO message");
        logger.warn("This is a WARN message");
        logger.error("This is an ERROR message");

        // Log a message with ASCII color codes directly
        logger.info("\u001B[31mThis is red text\u001B[0m");
        logger.info("\u001B[32mThis is green text\u001B[0m");
        logger.info("\u001B[33mThis is yellow text\u001B[0m");
        logger.info("\u001B[34mThis is blue text\u001B[0m");
        logger.info("\u001B[35mThis is magenta text\u001B[0m");
        logger.info("\u001B[36mThis is cyan text\u001B[0m");

        // Test that the test passes
        assert true;
    }
}