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
 * Unit tests for ProductsSilverTransformer.
 */
public class ProductsSilverTransformerTest extends SparkTestBase {
    
    @Test
    public void testDataQualityRules_ValidProducts_ShouldPass() {
        // Arrange
        Dataset<Row> inputProducts = createValidProductsTestData();
        ProductsSilverTransformer transformer = new ProductsSilverTransformer(spark);
        
        // Act
        BaseSilverTransformer.DataQualityResult result = 
            transformer.applyDataQualityRules(inputProducts);
        
        // Assert
        assertEquals("Expected all products to be valid", 3, result.validRecords.count());
        assertEquals("Expected no invalid products", 0, result.invalidRecords.count());
    }
    
    @Test
    public void testDataQualityRules_NegativeWeight_ShouldQuarantine() {
        // Arrange
        Dataset<Row> inputProducts = createProductsWithNegativeWeight();
        ProductsSilverTransformer transformer = new ProductsSilverTransformer(spark);
        
        // Act
        BaseSilverTransformer.DataQualityResult result = 
            transformer.applyDataQualityRules(inputProducts);
        
        // Assert
        assertEquals("Expected 1 invalid product", 1, result.invalidRecords.count());
        
        String reason = result.invalidRecords.select("quarantine_reason").first().getString(0);
        assertTrue("Should mention invalid weight", reason.contains("Invalid weight"));
    }
    
    @Test
    public void testEnrichment_VolumeCalculation_ShouldBeCorrect() {
        // Arrange
        Dataset<Row> inputProducts = createProductsForVolumeTest();
        ProductsSilverTransformer transformer = new ProductsSilverTransformer(spark);
        
        // Act
        Dataset<Row> enriched = transformer.enrichData(inputProducts);
        
        // Assert - 10 x 20 x 30 = 6000 cubic cm
        Integer volume = enriched
            .filter("product_id = 'prod_1'")
            .select("volume_cubic_cm")
            .first()
            .getInt(0);
        
        assertEquals("Volume should be 6000 cubic cm", Integer.valueOf(6000), volume);
    }
    
    @Test
    public void testEnrichment_WeightCategory_ShouldClassifyCorrectly() {
        // Arrange
        Dataset<Row> inputProducts = createProductsForWeightCategoryTest();
        ProductsSilverTransformer transformer = new ProductsSilverTransformer(spark);
        
        // Act
        Dataset<Row> enriched = transformer.enrichData(inputProducts);
        
        // Assert
        String lightCategory = enriched
            .filter("product_id = 'light_prod'")
            .select("weight_category")
            .first()
            .getString(0);
        
        assertEquals("300g should be Light", "Light", lightCategory);
        
        String heavyCategory = enriched
            .filter("product_id = 'heavy_prod'")
            .select("weight_category")
            .first()
            .getString(0);
        
        assertEquals("6000g should be Very Heavy", "Very Heavy", heavyCategory);
    }
    
    @Test
    public void testEnrichment_SizeCategory_ShouldClassifyCorrectly() {
        // Arrange
        Dataset<Row> inputProducts = createProductsForSizeCategoryTest();
        ProductsSilverTransformer transformer = new ProductsSilverTransformer(spark);
        
        // Act
        Dataset<Row> enriched = transformer.enrichData(inputProducts);
        
        // Assert
        String sizeCategory = enriched
            .filter("product_id = 'small_prod'")
            .select("size_category")
            .first()
            .getString(0);
        
        assertEquals("500 cubic cm should be Small", "Small", sizeCategory);
    }
    
    @Test
    public void testDeduplication_DuplicateProducts_ShouldKeepOne() {
        // Arrange
        Dataset<Row> inputProducts = createProductsWithDuplicates();
        ProductsSilverTransformer transformer = new ProductsSilverTransformer(spark);
        
        // Act
        Dataset<Row> deduped = transformer.deduplicateRecords(inputProducts);
        
        // Assert
        assertEquals("Should have 2 unique products", 2, deduped.count());
    }
    
    // ========== Helper Methods ==========
    
    private Dataset<Row> createValidProductsTestData() {
        StructType schema = createProductSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("prod_1", "electronics", 40, 287, 1, 1000, 20, 10, 15),
            RowFactory.create("prod_2", "furniture", 44, 276, 1, 5000, 100, 50, 80),
            RowFactory.create("prod_3", "toys", 30, 200, 2, 200, 10, 10, 10)
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createProductsWithNegativeWeight() {
        StructType schema = createProductSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("prod_1", "electronics", 40, 287, 1, -1000, 20, 10, 15)
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createProductsForVolumeTest() {
        StructType schema = createProductSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("prod_1", "test", 40, 287, 1, 1000, 10, 20, 30)  // 10*20*30 = 6000
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createProductsForWeightCategoryTest() {
        StructType schema = createProductSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("light_prod", "test", 40, 287, 1, 300, 10, 10, 10),    // Light
            RowFactory.create("heavy_prod", "test", 40, 287, 1, 6000, 50, 50, 50)    // Very Heavy
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createProductsForSizeCategoryTest() {
        StructType schema = createProductSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("small_prod", "test", 40, 287, 1, 1000, 5, 10, 10)  // 5*10*10 = 500 (Small)
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private Dataset<Row> createProductsWithDuplicates() {
        StructType schema = createProductSchema();
        
        List<Row> data = Arrays.asList(
            RowFactory.create("prod_123", "electronics", 40, 287, 1, 1000, 20, 10, 15),
            RowFactory.create("prod_123", "electronics", 40, 287, 1, 1000, 20, 10, 15),  // Duplicate
            RowFactory.create("prod_456", "furniture", 44, 276, 1, 5000, 100, 50, 80)
        );
        
        return spark.createDataFrame(data, schema);
    }
    
    private StructType createProductSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("product_id", DataTypes.StringType, true),
            DataTypes.createStructField("product_category_name", DataTypes.StringType, true),
            DataTypes.createStructField("product_name_lenght", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_description_lenght", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_photos_qty", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_weight_g", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_length_cm", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_height_cm", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_width_cm", DataTypes.IntegerType, true)
        ));
    }
}