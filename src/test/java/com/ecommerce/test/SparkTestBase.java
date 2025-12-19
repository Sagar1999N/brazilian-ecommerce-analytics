package com.ecommerce.test;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Base class for Spark unit tests.
 * 
 * Provides a shared SparkSession for all tests to improve performance.
 * SparkSession is expensive to create, so we reuse it across tests.
 * 
 * Design Pattern: Template Method + Shared Fixture
 * 
 * Usage:
 * ```java
 * public class MyTransformerTest extends SparkTestBase {
 *     @Test
 *     public void testTransformation() {
 *         Dataset<Row> input = createTestData();
 *         Dataset<Row> result = myTransformer.transform(input);
 *         // assertions
 *     }
 * }
 * ```
 */
public abstract class SparkTestBase {
    
    protected static SparkSession spark;
    
    /**
     * Creates a shared SparkSession for all tests.
     * Called once before any tests run.
     */
    @BeforeClass
    public static void setUpClass() {
        if (spark == null) {
            spark = SparkSession.builder()
                .appName("SparkTestSession")
                .master("local[2]")  // Use 2 cores for parallel testing
                .config("spark.ui.enabled", "false")  // Disable Spark UI for tests
                .config("spark.sql.shuffle.partitions", "2")  // Reduce shuffles for tests
                .config("spark.sql.warehouse.dir", "target/spark-warehouse")  // Test warehouse
                .config("spark.driver.bindAddress", "127.0.0.1")  // Localhost only
                .getOrCreate();
            
            // Set log level to WARN to reduce test output noise
            spark.sparkContext().setLogLevel("WARN");
        }
    }
    
    /**
     * Hook for test-specific setup.
     * Override this in subclasses if needed.
     */
    @Before
    public void setUp() {
        // Subclasses can override for test-specific setup
    }
    
    /**
     * Hook for test-specific cleanup.
     * Override this in subclasses if needed.
     */
    @After
    public void tearDown() {
        // Subclasses can override for test-specific cleanup
        // Note: We don't stop the SparkSession here to reuse it
    }
    
    /**
     * Helper method to clear test data directories.
     * Useful for cleaning up between tests.
     */
    protected void cleanupTestData(String path) {
        try {
            java.nio.file.Path testPath = java.nio.file.Paths.get(path);
            if (java.nio.file.Files.exists(testPath)) {
                // Delete recursively
                java.nio.file.Files.walk(testPath)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            java.nio.file.Files.delete(p);
                        } catch (Exception e) {
                            // Ignore deletion errors in tests
                        }
                    });
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}