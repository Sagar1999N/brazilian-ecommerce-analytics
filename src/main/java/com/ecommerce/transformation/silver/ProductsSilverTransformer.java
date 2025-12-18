package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.AppConfig;

import static org.apache.spark.sql.functions.*;

/**
 * Transforms Products from Bronze to Silver layer.
 * 
 * Responsibilities:
 * 1. Data Quality Validation (null checks, negative values, logical constraints)
 * 2. Deduplication
 * 3. Data Enrichment (calculated fields, categorization)
 * 4. Dead Letter Queue for bad records
 * 
 * Design Patterns:
 * - Idempotency: Safe to rerun
 * - DLQ: Invalid records quarantined
 * - Data Quality Gates: Clear validation rules
 */
public class ProductsSilverTransformer {
    private static final Logger logger = LoggerFactory.getLogger(ProductsSilverTransformer.class);
    
    private final SparkSession spark;
    private final String bronzePath;
    private final String silverPath;
    private final String quarantinePath;
    
    public ProductsSilverTransformer(SparkSession spark) {
        this.spark = spark;
        this.bronzePath = AppConfig.getString("app.data.bronze.path", "data/staging/");
        this.silverPath = AppConfig.getString("app.data.silver.path", "data/silver/");
        this.quarantinePath = AppConfig.getString("app.data.quarantine.path", "data/quarantine/");
    }
    
    /**
     * Main transformation method
     */
    public TransformationResult transform() {
        logger.info("=== Starting Bronze -> Silver Transformation for Products ===");
        
        TransformationResult result = new TransformationResult();
        
        try {
            // Step 1: Read Bronze data
            logger.info("Step 1: Reading Bronze data from: {}", bronzePath + "products");
            Dataset<Row> bronzeProducts = spark.read().parquet(bronzePath + "products");
            long totalRecords = bronzeProducts.count();
            logger.info("Bronze records read: {}", totalRecords);
            result.setTotalRecords(totalRecords);
            
            // Step 2: Add metadata columns
            Dataset<Row> productsWithMetadata = addMetadataColumns(bronzeProducts);
            
            // Step 3: Data Quality Validation
            logger.info("Step 2: Applying data quality rules...");
            DataQualityResult dqResult = applyDataQualityRules(productsWithMetadata);
            
            Dataset<Row> validProducts = dqResult.validRecords;
            Dataset<Row> invalidProducts = dqResult.invalidRecords;
            
            result.setValidRecords(validProducts.count());
            result.setInvalidRecords(invalidProducts.count());
            
            logger.info("Valid records: {}", result.getValidRecords());
            logger.info("Invalid records: {}", result.getInvalidRecords());
            
            // Step 4: Quarantine bad records
            if (result.getInvalidRecords() > 0) {
                logger.warn("Quarantining {} invalid records", result.getInvalidRecords());
                quarantineInvalidRecords(invalidProducts);
            }
            
            // Step 5: Deduplication
            logger.info("Step 3: Deduplicating records...");
            Dataset<Row> dedupedProducts = deduplicateProducts(validProducts);
            long afterDedup = dedupedProducts.count();
            result.setDuplicatesRemoved(result.getValidRecords() - afterDedup);
            logger.info("Duplicates removed: {}", result.getDuplicatesRemoved());
            
            // Step 6: Data Enrichment
            logger.info("Step 4: Enriching product data...");
            Dataset<Row> enrichedProducts = enrichProducts(dedupedProducts);
            
            // Step 7: Write to Silver
            logger.info("Step 5: Writing to Silver layer: {}", silverPath + "products");
            writeSilver(enrichedProducts);
            
            logger.info("=== Bronze -> Silver Transformation Completed Successfully ===");
            result.setSuccess(true);
            
        } catch (Exception e) {
            logger.error("Transformation failed", e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Add metadata columns
     */
    private Dataset<Row> addMetadataColumns(Dataset<Row> df) {
        return df
            .withColumn("silver_processed_timestamp", current_timestamp())
            .withColumn("silver_processing_date", current_date())
            .withColumn("data_source", lit("brazilian_ecommerce"));
    }
    
    /**
     * Apply data quality rules
     */
    private DataQualityResult applyDataQualityRules(Dataset<Row> products) {
        logger.info("Applying data quality validation rules...");
        
        Dataset<Row> productsWithValidation = products
            // Check for null primary key
            .withColumn("is_product_id_null", col("product_id").isNull())
            
            // Check for negative or zero physical dimensions
            .withColumn("is_weight_valid",
                col("product_weight_g").isNull().or(
                    col("product_weight_g").gt(0)
                )
            )
            
            .withColumn("is_dimensions_valid",
                // Either all dimensions are null, or all are positive
                (col("product_length_cm").isNull()
                    .and(col("product_height_cm").isNull())
                    .and(col("product_width_cm").isNull())
                ).or(
                    col("product_length_cm").gt(0)
                    .and(col("product_height_cm").gt(0))
                    .and(col("product_width_cm").gt(0))
                )
            )
            
            // Overall validation flag
            .withColumn("is_valid_record",
                not(col("is_product_id_null"))
                .and(col("is_weight_valid"))
                .and(col("is_dimensions_valid"))
            );
        
        // Split into valid and invalid
        Dataset<Row> validRecords = productsWithValidation
            .filter(col("is_valid_record").equalTo(true))
            .drop("is_product_id_null", "is_weight_valid", "is_dimensions_valid", "is_valid_record");
        
        Dataset<Row> invalidRecords = productsWithValidation
            .filter(col("is_valid_record").equalTo(false))
            .withColumn("quarantine_reason", 
                concat_ws("; ",
                    when(col("is_product_id_null"), lit("Null product_id")).otherwise(lit("")),
                    when(not(col("is_weight_valid")), lit("Invalid weight (negative or zero)")).otherwise(lit("")),
                    when(not(col("is_dimensions_valid")), lit("Invalid dimensions")).otherwise(lit(""))
                )
            )
            .withColumn("quarantine_timestamp", current_timestamp());
        
        return new DataQualityResult(validRecords, invalidRecords);
    }
    
    /**
     * Deduplicate products
     */
    private Dataset<Row> deduplicateProducts(Dataset<Row> products) {
        logger.info("Deduplicating products by product_id...");
        
        // Keep the first occurrence
        return products
            .withColumn("row_num", 
                row_number().over(
                    org.apache.spark.sql.expressions.Window
                        .partitionBy("product_id")
                        .orderBy(col("product_id").asc())
                )
            )
            .filter(col("row_num").equalTo(1))
            .drop("row_num");
    }
    
    /**
     * Enrich product data with calculated fields
     */
    private Dataset<Row> enrichProducts(Dataset<Row> products) {
        logger.info("Enriching product data with calculated fields...");
        
        return products
            // Calculate volume in cubic centimeters
            .withColumn("volume_cubic_cm",
                when(col("product_length_cm").isNotNull()
                    .and(col("product_height_cm").isNotNull())
                    .and(col("product_width_cm").isNotNull()),
                    col("product_length_cm")
                        .multiply(col("product_height_cm"))
                        .multiply(col("product_width_cm"))
                ).otherwise(lit(null))
            )
            
            // Calculate volume in liters
            .withColumn("volume_liters",
                when(col("volume_cubic_cm").isNotNull(),
                    round(col("volume_cubic_cm").divide(1000), 2)
                ).otherwise(lit(null))
            )
            
            // Weight category
            .withColumn("weight_category",
                when(col("product_weight_g").isNull(), "Unknown")
                .when(col("product_weight_g").leq(500), "Light")
                .when(col("product_weight_g").leq(2000), "Medium")
                .when(col("product_weight_g").leq(5000), "Heavy")
                .otherwise("Very Heavy")
            )
            
            // Size category based on volume
            .withColumn("size_category",
                when(col("volume_cubic_cm").isNull(), "Unknown")
                .when(col("volume_cubic_cm").leq(1000), "Small")
                .when(col("volume_cubic_cm").leq(10000), "Medium")
                .when(col("volume_cubic_cm").leq(50000), "Large")
                .otherwise("Extra Large")
            )
            
            // Standardize category name (lowercase, trim)
            .withColumn("product_category_name",
                when(col("product_category_name").isNotNull(),
                    lower(trim(col("product_category_name")))
                ).otherwise(lit("uncategorized"))
            )
            
            // Shipping complexity score (higher = more complex/expensive to ship)
            .withColumn("shipping_complexity_score",
                when(col("product_weight_g").isNotNull()
                    .and(col("volume_cubic_cm").isNotNull()),
                    // Weighted combination of weight and volume
                    round(
                        (col("product_weight_g").divide(1000).multiply(0.6))
                        .plus(col("volume_liters").multiply(0.4)),
                        2
                    )
                ).otherwise(lit(null))
            )
            
            // Flag for products with missing dimensions
            .withColumn("has_complete_dimensions",
                col("product_weight_g").isNotNull()
                .and(col("product_length_cm").isNotNull())
                .and(col("product_height_cm").isNotNull())
                .and(col("product_width_cm").isNotNull())
            );
    }
    
    /**
     * Quarantine invalid records
     */
    private void quarantineInvalidRecords(Dataset<Row> invalidRecords) {
        logger.info("Writing invalid records to quarantine...");
        
        String quarantineProductsPath = quarantinePath + "products";
        
        invalidRecords
            .write()
            .mode(SaveMode.Append)
            .partitionBy("silver_processing_date")
            .parquet(quarantineProductsPath);
        
        logger.info("Invalid records quarantined at: {}", quarantineProductsPath);
    }
    
    /**
     * Write to Silver layer
     */
    private void writeSilver(Dataset<Row> silverProducts) {
        String silverProductsPath = silverPath + "products";
        
        silverProducts
            .write()
            .mode(SaveMode.Overwrite)
            .parquet(silverProductsPath);
        
        logger.info("Silver data written successfully to: {}", silverProductsPath);
    }
    
    // Helper classes
    private static class DataQualityResult {
        Dataset<Row> validRecords;
        Dataset<Row> invalidRecords;
        
        DataQualityResult(Dataset<Row> validRecords, Dataset<Row> invalidRecords) {
            this.validRecords = validRecords;
            this.invalidRecords = invalidRecords;
        }
    }
    
    public static class TransformationResult {
        private boolean success;
        private long totalRecords;
        private long validRecords;
        private long invalidRecords;
        private long duplicatesRemoved;
        private String errorMessage;
        
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        
        public long getTotalRecords() { return totalRecords; }
        public void setTotalRecords(long totalRecords) { this.totalRecords = totalRecords; }
        
        public long getValidRecords() { return validRecords; }
        public void setValidRecords(long validRecords) { this.validRecords = validRecords; }
        
        public long getInvalidRecords() { return invalidRecords; }
        public void setInvalidRecords(long invalidRecords) { this.invalidRecords = invalidRecords; }
        
        public long getDuplicatesRemoved() { return duplicatesRemoved; }
        public void setDuplicatesRemoved(long duplicatesRemoved) { 
            this.duplicatesRemoved = duplicatesRemoved; 
        }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public void logSummary(Logger logger) {
            logger.info("=== Products Transformation Summary ===");
            logger.info("Success: {}", success);
            logger.info("Total Records: {}", totalRecords);
            logger.info("Valid Records: {}", validRecords);
            logger.info("Invalid Records: {}", invalidRecords);
            logger.info("Duplicates Removed: {}", duplicatesRemoved);
            if (!success) {
                logger.error("Error: {}", errorMessage);
            }
            logger.info("=======================================");
        }
    }
}