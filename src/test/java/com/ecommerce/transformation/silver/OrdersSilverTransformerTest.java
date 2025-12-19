package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import com.ecommerce.test.SparkTestBase;
import com.ecommerce.transformation.silver.BaseSilverTransformer;
import com.ecommerce.transformation.silver.OrdersSilverTransformer;
import com.ecommerce.transformation.silver.BaseSilverTransformer.DataQualityResult;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for OrdersSilverTransformer.
 * 
 * Tests cover:
 * - Data quality validation rules
 * - Deduplication logic
 * - Enrichment calculations
 * - Edge cases and error handling
 * 
 * Testing Strategy:
 * - Create small, focused test datasets manually
 * - Test one aspect per test method
 * - Use descriptive test names
 * - Assert expected vs actual results
 */
public class OrdersSilverTransformerTest extends SparkTestBase {
    
    /**
     * Test: Valid orders pass data quality checks
     */
    @Test
    public void testDataQualityRules_ValidOrders_ShouldPass() {
        // Arrange: Create test data with valid orders
        Dataset<Row> inputOrders = createValidOrdersTestData();
        
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act: Apply data quality rules
        BaseSilverTransformer.DataQualityResult result = 
            transformer.applyDataQualityRules(inputOrders);
        
        // Assert: All orders should be valid
        long validCount = result.validRecords.count();
        long invalidCount = result.invalidRecords.count();
        
        assertEquals("Expected all orders to be valid", 3, validCount);
        assertEquals("Expected no invalid orders", 0, invalidCount);
    }
    
    /**
     * Test: Orders with null order_id should be quarantined
     */
    @Test
    public void testDataQualityRules_NullOrderId_ShouldQuarantine() {
        // Arrange: Create test data with null order_id
        Dataset<Row> inputOrders = createOrdersWithNullOrderId();
        
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act: Apply data quality rules
        BaseSilverTransformer.DataQualityResult result = 
            transformer.applyDataQualityRules(inputOrders);
        
        // Assert: Orders with null order_id should be invalid
        long invalidCount = result.invalidRecords.count();
        assertEquals("Expected 1 invalid order", 1, invalidCount);
        
        // Check quarantine reason
        String quarantineReason = result.invalidRecords
            .select("quarantine_reason")
            .first()
            .getString(0);
        
        assertTrue("Quarantine reason should mention null order_id", 
            quarantineReason.contains("Null order_id"));
    }
    
    /**
     * Test: Orders with invalid status should be quarantined
     */
    @Test
    public void testDataQualityRules_InvalidStatus_ShouldQuarantine() {
        // Arrange: Create test data with invalid status
        Dataset<Row> inputOrders = createOrdersWithInvalidStatus();
        
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act: Apply data quality rules
        BaseSilverTransformer.DataQualityResult result = 
            transformer.applyDataQualityRules(inputOrders);
        
        // Assert: Invalid status orders should be quarantined
        long invalidCount = result.invalidRecords.count();
        assertEquals("Expected 1 invalid order", 1, invalidCount);
        
        String quarantineReason = result.invalidRecords
            .select("quarantine_reason")
            .first()
            .getString(0);
        
        assertTrue("Quarantine reason should mention invalid status", 
            quarantineReason.contains("Invalid order_status"));
    }
    
    /**
     * Test: Duplicate orders should be deduplicated, keeping the latest
     */
    @Test
    public void testDeduplication_DuplicateOrders_ShouldKeepLatest() {
        // Arrange: Create test data with duplicates
        Dataset<Row> inputOrders = createOrdersWithDuplicates();
        
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act: Deduplicate
        Dataset<Row> dedupedOrders = transformer.deduplicateRecords(inputOrders);
        
        // Assert: Should have only 2 unique orders
        long uniqueCount = dedupedOrders.count();
        assertEquals("Expected 2 unique orders after deduplication", 2, uniqueCount);
        
        // Verify we kept the latest record (status = "delivered")
        String keptStatus = dedupedOrders
            .filter("order_id = 'order_123'")
            .select("order_status")
            .first()
            .getString(0);
        
        assertEquals("Should keep the latest record", "delivered", keptStatus);
    }
    
    /**
     * Test: Enrichment should calculate delivery_time_days correctly
     */
    @Test
    public void testEnrichment_DeliveryTime_ShouldCalculateCorrectly() {
        // Arrange: Create test data with known dates
        Dataset<Row> inputOrders = createOrdersForEnrichmentTest();
        
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act: Enrich data
        Dataset<Row> enrichedOrders = transformer.enrichData(inputOrders);
        
        // Assert: Check delivery_time_days calculation
        Row result = enrichedOrders
            .filter("order_id = 'order_with_delivery'")
            .select("delivery_time_days")
            .first();
        
        Integer deliveryDays = result.isNullAt(0) ? null : result.getInt(0);
        assertNotNull("delivery_time_days should not be null", deliveryDays);
        assertEquals("Expected 5 days delivery time", Integer.valueOf(5), deliveryDays);
    }
    
    /**
     * Test: Enrichment should identify late deliveries
     */
    @Test
    public void testEnrichment_LateDelivery_ShouldFlagCorrectly() {
        // Arrange: Create test data with late delivery
        Dataset<Row> inputOrders = createOrdersWithLateDelivery();
        
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act: Enrich data
        Dataset<Row> enrichedOrders = transformer.enrichData(inputOrders);
        
        // Assert: Late delivery flag should be true
        Row result = enrichedOrders
            .filter("order_id = 'late_order'")
            .select("is_late_delivery")
            .first();
        
        Boolean isLate = result.isNullAt(0) ? null : result.getBoolean(0);
        assertNotNull("is_late_delivery should not be null", isLate);
        assertTrue("Order should be flagged as late", isLate);
    }
    
    /**
     * Test: Enrichment should extract date parts correctly
     */
    @Test
    public void testEnrichment_DateParts_ShouldExtractCorrectly() {
        // Arrange: Create test data with known date
        Dataset<Row> inputOrders = createOrdersForDatePartTest();
        
        OrdersSilverTransformer transformer = new OrdersSilverTransformer(spark);
        
        // Act: Enrich data
        Dataset<Row> enrichedOrders = transformer.enrichData(inputOrders);
        
        // Assert: Check date parts (order placed on 2024-06-15)
        Row result = enrichedOrders
            .filter("order_id = 'date_test_order'")
            .select("order_year", "order_month", "order_quarter")
            .first();
        
        assertEquals("Expected year 2024", 2024, result.getInt(0));
        assertEquals("Expected month 6", 6, result.getInt(1));
        assertEquals("Expected quarter 2", 2, result.getInt(2));
    }
    
    // ========== Helper Methods to Create Test Data ==========
    
    private Dataset<Row> createValidOrdersTestData() {
        StructType schema = createOrderSchema();
        
        List<Row> data = Arrays.asList(
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
            ),
            RowFactory.create(
                "order_3", "customer_3", "canceled",
                Timestamp.valueOf("2024-01-03 10:00:00"),
                null, null, null,
                Timestamp.valueOf("2024-01-10 00:00:00")
            )
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createOrdersWithNullOrderId() {
        StructType schema = createOrderSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create(
                null,  // Null order_id
                "customer_1", "delivered",
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                Timestamp.valueOf("2024-01-02 10:00:00"),
                Timestamp.valueOf("2024-01-05 10:00:00"),
                Timestamp.valueOf("2024-01-07 00:00:00")
            )
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createOrdersWithInvalidStatus() {
        StructType schema = createOrderSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create(
                "order_1", "customer_1", "invalid_status",  // Invalid status
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                Timestamp.valueOf("2024-01-02 10:00:00"),
                Timestamp.valueOf("2024-01-05 10:00:00"),
                Timestamp.valueOf("2024-01-07 00:00:00")
            )
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createOrdersWithDuplicates() {
        StructType schema = createOrderSchema();
        
        List<Row> data = Arrays.asList(
            // First occurrence of order_123 (older)
            RowFactory.create(
                "order_123", "customer_1", "shipped",
                Timestamp.valueOf("2024-01-01 10:00:00"),  // Older timestamp
                Timestamp.valueOf("2024-01-01 11:00:00"),
                null, null,
                Timestamp.valueOf("2024-01-07 00:00:00")
            ),
            // Second occurrence of order_123 (newer - should be kept)
            RowFactory.create(
                "order_123", "customer_1", "delivered",
                Timestamp.valueOf("2024-01-05 10:00:00"),  // Newer timestamp
                Timestamp.valueOf("2024-01-05 11:00:00"),
                Timestamp.valueOf("2024-01-06 10:00:00"),
                Timestamp.valueOf("2024-01-08 10:00:00"),
                Timestamp.valueOf("2024-01-07 00:00:00")
            ),
            // Different order
            RowFactory.create(
                "order_456", "customer_2", "delivered",
                Timestamp.valueOf("2024-01-02 10:00:00"),
                Timestamp.valueOf("2024-01-02 11:00:00"),
                Timestamp.valueOf("2024-01-03 10:00:00"),
                Timestamp.valueOf("2024-01-06 10:00:00"),
                Timestamp.valueOf("2024-01-10 00:00:00")
            )
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createOrdersForEnrichmentTest() {
        StructType schema = createOrderSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create(
                "order_with_delivery", "customer_1", "delivered",
                Timestamp.valueOf("2024-01-01 10:00:00"),  // Purchase
                Timestamp.valueOf("2024-01-01 11:00:00"),
                Timestamp.valueOf("2024-01-02 10:00:00"),
                Timestamp.valueOf("2024-01-06 10:00:00"),  // Delivery (5 days later)
                Timestamp.valueOf("2024-01-10 00:00:00")
            )
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createOrdersWithLateDelivery() {
        StructType schema = createOrderSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create(
                "late_order", "customer_1", "delivered",
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                Timestamp.valueOf("2024-01-02 10:00:00"),
                Timestamp.valueOf("2024-01-10 10:00:00"),  // Delivered
                Timestamp.valueOf("2024-01-07 00:00:00")   // Expected (3 days late!)
            )
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createOrdersForDatePartTest() {
        StructType schema = createOrderSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create(
                "date_test_order", "customer_1", "delivered",
                Timestamp.valueOf("2024-06-15 10:00:00"),  // June 15, 2024 (Q2)
                Timestamp.valueOf("2024-06-15 11:00:00"),
                Timestamp.valueOf("2024-06-16 10:00:00"),
                Timestamp.valueOf("2024-06-20 10:00:00"),
                Timestamp.valueOf("2024-06-25 00:00:00")
            )
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private StructType createOrderSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("order_id", DataTypes.StringType, true),
            DataTypes.createStructField("customer_id", DataTypes.StringType, true),
            DataTypes.createStructField("order_status", DataTypes.StringType, true),
            DataTypes.createStructField("order_purchase_timestamp", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_approved_at", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_delivered_carrier_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_delivered_customer_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_estimated_delivery_date", DataTypes.TimestampType, true)
        ));
    }
}