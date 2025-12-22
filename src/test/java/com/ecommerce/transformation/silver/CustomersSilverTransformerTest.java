package com.ecommerce.transformation.silver;

import com.ecommerce.test.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for CustomersSilverTransformer.
 */
public class CustomersSilverTransformerTest extends SparkTestBase {
    
    @Test
    public void testDataQualityRules_ValidCustomers_ShouldPass() {
        // Arrange
        Dataset<Row> inputCustomers = createValidCustomersTestData();
        CustomersSilverTransformer transformer = new CustomersSilverTransformer(spark);
        
        // Act
        BaseSilverTransformer.DataQualityResult result = 
            transformer.applyDataQualityRules(inputCustomers);
        
        // Assert
        assertEquals("Expected all customers to be valid", 3, result.validRecords.count());
        assertEquals("Expected no invalid customers", 0, result.invalidRecords.count());
    }
    
    @Test
    public void testDataQualityRules_NullCustomerId_ShouldQuarantine() {
        // Arrange
        Dataset<Row> inputCustomers = createCustomersWithNullId();
        CustomersSilverTransformer transformer = new CustomersSilverTransformer(spark);
        
        // Act
        BaseSilverTransformer.DataQualityResult result = 
            transformer.applyDataQualityRules(inputCustomers);
        
        // Assert
        assertEquals("Expected 1 invalid customer", 1, result.invalidRecords.count());
        
        String reason = result.invalidRecords.select("quarantine_reason").first().getString(0);
        assertTrue("Should mention null customer_id", reason.contains("Null customer_id"));
    }
    
    @Test
    public void testDataQualityRules_InvalidStateCode_ShouldQuarantine() {
        // Arrange
        Dataset<Row> inputCustomers = createCustomersWithInvalidState();
        CustomersSilverTransformer transformer = new CustomersSilverTransformer(spark);
        
        // Act
        BaseSilverTransformer.DataQualityResult result = 
            transformer.applyDataQualityRules(inputCustomers);
        
        // Assert
        assertEquals("Expected 1 invalid customer", 1, result.invalidRecords.count());
        
        String reason = result.invalidRecords.select("quarantine_reason").first().getString(0);
        assertTrue("Should mention invalid state", reason.contains("Invalid state code"));
    }
    
    @Test
    public void testEnrichment_GeographicRegion_ShouldMapCorrectly() {
        // Arrange
        Dataset<Row> inputCustomers = createCustomersForRegionTest();
        CustomersSilverTransformer transformer = new CustomersSilverTransformer(spark);
        
        // Act
        Dataset<Row> enriched = transformer.enrichData(inputCustomers);
        
        // Assert - SP should be Southeast
        String region = enriched
            .filter("customer_id = 'cust_sp'")
            .select("geographic_region")
            .first()
            .getString(0);
        
        assertEquals("SP should be Southeast", "Southeast", region);
        
        // RS should be South
        String regionRS = enriched
            .filter("customer_id = 'cust_rs'")
            .select("geographic_region")
            .first()
            .getString(0);
        
        assertEquals("RS should be South", "South", regionRS);
    }
    
    @Test
    public void testEnrichment_CityStandardization_ShouldTitleCase() {
        // Arrange
        Dataset<Row> inputCustomers = createCustomersForStandardizationTest();
        CustomersSilverTransformer transformer = new CustomersSilverTransformer(spark);
        
        // Act
        Dataset<Row> enriched = transformer.enrichData(inputCustomers);
        
        // Assert
        String city = enriched
            .filter("customer_id = 'cust_1'")
            .select("customer_city")
            .first()
            .getString(0);
        
        assertEquals("City should be title case", "Sao Paulo", city);
    }
    
    @Test
    public void testDeduplication_DuplicateCustomers_ShouldKeepOne() {
        // Arrange
        Dataset<Row> inputCustomers = createCustomersWithDuplicates();
        CustomersSilverTransformer transformer = new CustomersSilverTransformer(spark);
        
        // Act
        Dataset<Row> deduped = transformer.deduplicateRecords(inputCustomers);
        
        // Assert
        assertEquals("Should have 2 unique customers", 2, deduped.count());
    }
    
    // ========== Helper Methods ==========
    
    private Dataset<Row> createValidCustomersTestData() {
        StructType schema = createCustomerSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("cust_1", "unique_1", "01310", "sao paulo", "SP"),
            RowFactory.create("cust_2", "unique_2", "20031", "rio de janeiro", "RJ"),
            RowFactory.create("cust_3", "unique_3", "30130", "belo horizonte", "MG")
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createCustomersWithNullId() {
        StructType schema = createCustomerSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create(null, "unique_1", "01310", "sao paulo", "SP")
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createCustomersWithInvalidState() {
        StructType schema = createCustomerSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("cust_1", "unique_1", "01310", "sao paulo", "XXX")  // Invalid
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createCustomersForRegionTest() {
        StructType schema = createCustomerSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("cust_sp", "unique_sp", "01310", "sao paulo", "SP"),
            RowFactory.create("cust_rs", "unique_rs", "90000", "porto alegre", "RS")
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createCustomersForStandardizationTest() {
        StructType schema = createCustomerSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("cust_1", "unique_1", "01310", "SAO PAULO", "sp")  // Mixed case
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createCustomersWithDuplicates() {
        StructType schema = createCustomerSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("cust_123", "unique_1", "01310", "sao paulo", "SP"),
            RowFactory.create("cust_123", "unique_1", "01310", "sao paulo", "SP"),  // Duplicate
            RowFactory.create("cust_456", "unique_2", "20031", "rio de janeiro", "RJ")
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private StructType createCustomerSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("customer_id", DataTypes.StringType, true),
            DataTypes.createStructField("customer_unique_id", DataTypes.StringType, true),
            DataTypes.createStructField("customer_zip_code_prefix", DataTypes.StringType, true),
            DataTypes.createStructField("customer_city", DataTypes.StringType, true),
            DataTypes.createStructField("customer_state", DataTypes.StringType, true)
        ));
    }
}