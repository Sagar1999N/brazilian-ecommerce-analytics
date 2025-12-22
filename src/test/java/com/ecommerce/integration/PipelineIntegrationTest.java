package com.ecommerce.integration;

import com.ecommerce.config.SparkSessionFactory;
import com.ecommerce.test.SparkTestBase;
import com.ecommerce.transformation.silver.OrdersSilverTransformer;
import com.ecommerce.transformation.silver.CustomersSilverTransformer;
import com.ecommerce.transformation.silver.ProductsSilverTransformer;
import com.ecommerce.transformation.silver.BaseSilverTransformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Integration test for the complete data pipeline.
 * 
 * Tests the full Bronze → Silver flow:
 * 1. Creates test Bronze data
 * 2. Runs all Silver transformers
 * 3. Verifies output data quality and structure
 * 
 * This simulates the real pipeline execution.
 */
public class PipelineIntegrationTest extends SparkTestBase {
    
    private static final String TEST_BRONZE_PATH = "target/test-data/bronze/";
    private static final String TEST_SILVER_PATH = "target/test-data/silver/";
    private static final String TEST_QUARANTINE_PATH = "target/test-data/quarantine/";
    
    @Before
    public void setUpTestData() {
        // Clean up any existing test data
        cleanupTestData(TEST_BRONZE_PATH);
        cleanupTestData(TEST_SILVER_PATH);
        cleanupTestData(TEST_QUARANTINE_PATH);
        
        // Create test Bronze data
        createBronzeTestData();
        
        // Set test paths in system properties (for AppConfig)
        System.setProperty("app.data.bronze.path", TEST_BRONZE_PATH);
        System.setProperty("app.data.silver.path", TEST_SILVER_PATH);
        System.setProperty("app.data.quarantine.path", TEST_QUARANTINE_PATH);
    }
    
    @After
    public void cleanUp() {
        // Clean up test data after test
        cleanupTestData(TEST_BRONZE_PATH);
        cleanupTestData(TEST_SILVER_PATH);
        cleanupTestData(TEST_QUARANTINE_PATH);
        
        // Clear system properties
        System.clearProperty("app.data.bronze.path");
        System.clearProperty("app.data.silver.path");
        System.clearProperty("app.data.quarantine.path");
    }
    
    /**
     * Test: Complete Bronze to Silver pipeline for Orders
     */
    @Test
    public void testCompletePipeline_Orders_ShouldTransformSuccessfully() {
        // Arrange - Bronze data already created in setup
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act - Run transformation
        BaseSilverTransformer.TransformationResult result = transformer.transform();
        
        // Assert - Transformation succeeded
        assertTrue("Transformation should succeed", result.isSuccess());
        assertTrue("Should have valid records", result.getValidRecords() > 0);
        
        // Verify Silver data exists
        Dataset<Row> silverOrders = spark.read().parquet(TEST_SILVER_PATH + "orders");
        assertTrue("Silver orders should have data", silverOrders.count() > 0);
        
        // Verify enrichment columns exist
        assertTrue("Should have delivery_time_days", 
            Arrays.asList(silverOrders.columns()).contains("delivery_time_days"));
        assertTrue("Should have is_late_delivery", 
            Arrays.asList(silverOrders.columns()).contains("is_late_delivery"));
        assertTrue("Should have metadata columns", 
            Arrays.asList(silverOrders.columns()).contains("silver_processed_timestamp"));
    }
    
    /**
     * Test: Complete Bronze to Silver pipeline for Customers
     */
    @Test
    public void testCompletePipeline_Customers_ShouldTransformSuccessfully() {
        // Arrange
        CustomersSilverTransformer transformer = new CustomersSilverTransformer(spark);
        
        // Act
        BaseSilverTransformer.TransformationResult result = transformer.transform();
        
        // Assert
        assertTrue("Transformation should succeed", result.isSuccess());
        assertTrue("Should have valid records", result.getValidRecords() > 0);
        
        // Verify Silver data
        Dataset<Row> silverCustomers = spark.read().parquet(TEST_SILVER_PATH + "customers");
        assertTrue("Silver customers should have data", silverCustomers.count() > 0);
        
        // Verify enrichment
        assertTrue("Should have geographic_region", 
            Arrays.asList(silverCustomers.columns()).contains("geographic_region"));
    }
    
    /**
     * Test: Complete Bronze to Silver pipeline for Products
     */
    @Test
    public void testCompletePipeline_Products_ShouldTransformSuccessfully() {
        // Arrange
        ProductsSilverTransformer transformer = new ProductsSilverTransformer(spark);
        
        // Act
        BaseSilverTransformer.TransformationResult result = transformer.transform();
        
        // Assert
        assertTrue("Transformation should succeed", result.isSuccess());
        assertTrue("Should have valid records", result.getValidRecords() > 0);
        
        // Verify Silver data
        Dataset<Row> silverProducts = spark.read().parquet(TEST_SILVER_PATH + "products");
        assertTrue("Silver products should have data", silverProducts.count() > 0);
        
        // Verify enrichment
        assertTrue("Should have volume_cubic_cm", 
            Arrays.asList(silverProducts.columns()).contains("volume_cubic_cm"));
        assertTrue("Should have weight_category", 
            Arrays.asList(silverProducts.columns()).contains("weight_category"));
    }
    
    /**
     * Test: Invalid records should be quarantined
     */
    @Test
    public void testPipeline_InvalidRecords_ShouldBeQuarantined() {
        // Arrange - Create Bronze data with invalid records
        createBronzeDataWithInvalidRecords();
        
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act
        BaseSilverTransformer.TransformationResult result = transformer.transform();
        
        // Assert
        assertTrue("Should have invalid records", result.getInvalidRecords() > 0);
        
        // Verify quarantine directory exists
        File quarantineDir = new File(TEST_QUARANTINE_PATH + "orders");
        assertTrue("Quarantine directory should exist", quarantineDir.exists());
        
        // Read and verify quarantined data
        Dataset<Row> quarantined = spark.read().parquet(TEST_QUARANTINE_PATH + "orders");
        assertTrue("Quarantine should have data", quarantined.count() > 0);
        assertTrue("Quarantine should have reason column", 
            Arrays.asList(quarantined.columns()).contains("quarantine_reason"));
    }
    
    /**
     * Test: Pipeline should be idempotent (can run multiple times)
     */
    @Test
    public void testPipeline_Idempotency_ShouldProduceSameResults() {
        // Arrange
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act - Run twice
        BaseSilverTransformer.TransformationResult result1 = transformer.transform();
        BaseSilverTransformer.TransformationResult result2 = transformer.transform();
        
        // Assert - Both should succeed with same record counts
        assertTrue("First run should succeed", result1.isSuccess());
        assertTrue("Second run should succeed", result2.isSuccess());
        assertEquals("Should have same valid records", 
            result1.getValidRecords(), result2.getValidRecords());
        
        // Verify Silver data is same
        Dataset<Row> silverOrders = spark.read().parquet(TEST_SILVER_PATH + "orders");
        assertEquals("Count should match first run", 
            result1.getValidRecords() - result1.getDuplicatesRemoved(), 
            silverOrders.count());
    }
    
    // ========== Helper Methods ==========
    
    private void createBronzeTestData() {
        // Create Orders
        StructType orderSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("order_id", DataTypes.StringType, false),
            DataTypes.createStructField("customer_id", DataTypes.StringType, false),
            DataTypes.createStructField("order_status", DataTypes.StringType, true),
            DataTypes.createStructField("order_purchase_timestamp", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_approved_at", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_delivered_carrier_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_delivered_customer_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_estimated_delivery_date", DataTypes.TimestampType, true)
        ));
        
        List<Row> orders = Arrays.asList(
            RowFactory.create(
                "order_1", "customer_1", "delivered",
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                Timestamp.valueOf("2024-01-02 10:00:00"),
                Timestamp.valueOf("2024-01-05 10:00:00"),
                Timestamp.valueOf("2024-01-07 00:00:00")
            ),
            RowFactory.create(
                "order_2", "customer_2", "shipped",
                Timestamp.valueOf("2024-01-02 10:00:00"),
                Timestamp.valueOf("2024-01-02 11:00:00"),
                Timestamp.valueOf("2024-01-03 10:00:00"),
                null,
                Timestamp.valueOf("2024-01-10 00:00:00")
            )
        );
        
        spark.createDataFrame(orders, orderSchema)
            .write()
            .mode(SaveMode.Overwrite)
            .parquet(TEST_BRONZE_PATH + "orders");
        
        // Create Customers
        StructType customerSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("customer_id", DataTypes.StringType, false),
            DataTypes.createStructField("customer_unique_id", DataTypes.StringType, false),
            DataTypes.createStructField("customer_zip_code_prefix", DataTypes.StringType, true),
            DataTypes.createStructField("customer_city", DataTypes.StringType, true),
            DataTypes.createStructField("customer_state", DataTypes.StringType, true)
        ));
        
        List<Row> customers = Arrays.asList(
            RowFactory.create("customer_1", "unique_1", "01310", "sao paulo", "SP"),
            RowFactory.create("customer_2", "unique_2", "20031", "rio de janeiro", "RJ")
        );
        
        spark.createDataFrame(customers, customerSchema)
            .write()
            .mode(SaveMode.Overwrite)
            .parquet(TEST_BRONZE_PATH + "customers");
        
        // Create Products
        StructType productSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("product_id", DataTypes.StringType, false),
            DataTypes.createStructField("product_category_name", DataTypes.StringType, true),
            DataTypes.createStructField("product_name_lenght", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_description_lenght", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_photos_qty", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_weight_g", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_length_cm", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_height_cm", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_width_cm", DataTypes.IntegerType, true)
        ));
        
        List<Row> products = Arrays.asList(
            RowFactory.create("prod_1", "electronics", 40, 287, 1, 1000, 20, 10, 15),
            RowFactory.create("prod_2", "furniture", 44, 276, 1, 5000, 100, 50, 80)
        );
        
        spark.createDataFrame(products, productSchema)
            .write()
            .mode(SaveMode.Overwrite)
            .parquet(TEST_BRONZE_PATH + "products");
    }
    
    private void createBronzeDataWithInvalidRecords() {
        StructType orderSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("order_id", DataTypes.StringType, true),
            DataTypes.createStructField("customer_id", DataTypes.StringType, true),
            DataTypes.createStructField("order_status", DataTypes.StringType, true),
            DataTypes.createStructField("order_purchase_timestamp", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_approved_at", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_delivered_carrier_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_delivered_customer_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_estimated_delivery_date", DataTypes.TimestampType, true)
        ));
        
        List<Row> orders = Arrays.asList(
            // Invalid: null order_id
            RowFactory.create(
                null, "customer_1", "delivered",
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                Timestamp.valueOf("2024-01-02 10:00:00"),
                Timestamp.valueOf("2024-01-05 10:00:00"),
                Timestamp.valueOf("2024-01-07 00:00:00")
            )
        );
        
        spark.createDataFrame(orders, orderSchema)
            .write()
            .mode(SaveMode.Overwrite)
            .parquet(TEST_BRONZE_PATH + "orders");
    }
}