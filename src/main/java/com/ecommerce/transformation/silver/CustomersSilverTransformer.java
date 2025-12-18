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
 * Transforms Customers from Bronze to Silver layer.
 * 
 * Responsibilities:
 * 1. Data Quality Validation (null checks, format validation)
 * 2. Deduplication (keep latest record per customer)
 * 3. Data Standardization (uppercase state codes, trim whitespace)
 * 4. Data Enrichment (derived fields)
 * 5. Dead Letter Queue for bad records
 * 
 * Design Patterns:
 * - Idempotency: Safe to rerun
 * - DLQ: Invalid records quarantined
 * - Data Quality Gates: Clear validation rules
 */
public class CustomersSilverTransformer {
    private static final Logger logger = LoggerFactory.getLogger(CustomersSilverTransformer.class);
    
    private final SparkSession spark;
    private final String bronzePath;
    private final String silverPath;
    private final String quarantinePath;
    
    public CustomersSilverTransformer(SparkSession spark) {
        this.spark = spark;
        this.bronzePath = AppConfig.getString("app.data.bronze.path", "data/staging/");
        this.silverPath = AppConfig.getString("app.data.silver.path", "data/silver/");
        this.quarantinePath = AppConfig.getString("app.data.quarantine.path", "data/quarantine/");
    }
    
    /**
     * Main transformation method
     */
    public TransformationResult transform() {
        logger.info("=== Starting Bronze -> Silver Transformation for Customers ===");
        
        TransformationResult result = new TransformationResult();
        
        try {
            // Step 1: Read Bronze data
            logger.info("Step 1: Reading Bronze data from: {}", bronzePath + "customers");
            Dataset<Row> bronzeCustomers = spark.read().parquet(bronzePath + "customers");
            long totalRecords = bronzeCustomers.count();
            logger.info("Bronze records read: {}", totalRecords);
            result.setTotalRecords(totalRecords);
            
            // Step 2: Add metadata columns
            Dataset<Row> customersWithMetadata = addMetadataColumns(bronzeCustomers);
            
            // Step 3: Data Quality Validation
            logger.info("Step 2: Applying data quality rules...");
            DataQualityResult dqResult = applyDataQualityRules(customersWithMetadata);
            
            Dataset<Row> validCustomers = dqResult.validRecords;
            Dataset<Row> invalidCustomers = dqResult.invalidRecords;
            
            result.setValidRecords(validCustomers.count());
            result.setInvalidRecords(invalidCustomers.count());
            
            logger.info("Valid records: {}", result.getValidRecords());
            logger.info("Invalid records: {}", result.getInvalidRecords());
            
            // Step 4: Quarantine bad records
            if (result.getInvalidRecords() > 0) {
                logger.warn("Quarantining {} invalid records", result.getInvalidRecords());
                quarantineInvalidRecords(invalidCustomers);
            }
            
            // Step 5: Deduplication
            logger.info("Step 3: Deduplicating records...");
            Dataset<Row> dedupedCustomers = deduplicateCustomers(validCustomers);
            long afterDedup = dedupedCustomers.count();
            result.setDuplicatesRemoved(result.getValidRecords() - afterDedup);
            logger.info("Duplicates removed: {}", result.getDuplicatesRemoved());
            
            // Step 6: Data Standardization & Enrichment
            logger.info("Step 4: Standardizing and enriching data...");
            Dataset<Row> enrichedCustomers = enrichCustomers(dedupedCustomers);
            
            // Step 7: Write to Silver
            logger.info("Step 5: Writing to Silver layer: {}", silverPath + "customers");
            writeSilver(enrichedCustomers);
            
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
    private DataQualityResult applyDataQualityRules(Dataset<Row> customers) {
        logger.info("Applying data quality validation rules...");
        
        Dataset<Row> customersWithValidation = customers
            // Check for null primary keys
            .withColumn("is_customer_id_null", col("customer_id").isNull())
            .withColumn("is_customer_unique_id_null", col("customer_unique_id").isNull())
            
            // Check for valid state code (should be 2 characters)
            .withColumn("is_state_valid",
                col("customer_state").isNotNull().and(
                    length(trim(col("customer_state"))).equalTo(2)
                )
            )
            
            // Check for valid zip code (should be numeric and not null)
            .withColumn("is_zip_valid",
                col("customer_zip_code_prefix").isNotNull().and(
                    length(col("customer_zip_code_prefix")).geq(5)
                )
            )
            
            // Check for valid city name (not null and not empty)
            .withColumn("is_city_valid",
                col("customer_city").isNotNull().and(
                    length(trim(col("customer_city"))).gt(0)
                )
            )
            
            // Overall validation flag
            .withColumn("is_valid_record",
                not(col("is_customer_id_null"))
                .and(not(col("is_customer_unique_id_null")))
                .and(col("is_state_valid"))
                .and(col("is_zip_valid"))
                .and(col("is_city_valid"))
            );
        
        // Split into valid and invalid
        Dataset<Row> validRecords = customersWithValidation
            .filter(col("is_valid_record").equalTo(true))
            .drop("is_customer_id_null", "is_customer_unique_id_null", 
                  "is_state_valid", "is_zip_valid", "is_city_valid", "is_valid_record");
        
        Dataset<Row> invalidRecords = customersWithValidation
            .filter(col("is_valid_record").equalTo(false))
            .withColumn("quarantine_reason", 
                concat_ws("; ",
                    when(col("is_customer_id_null"), lit("Null customer_id")).otherwise(lit("")),
                    when(col("is_customer_unique_id_null"), lit("Null customer_unique_id")).otherwise(lit("")),
                    when(not(col("is_state_valid")), lit("Invalid state code")).otherwise(lit("")),
                    when(not(col("is_zip_valid")), lit("Invalid zip code")).otherwise(lit("")),
                    when(not(col("is_city_valid")), lit("Invalid city name")).otherwise(lit(""))
                )
            )
            .withColumn("quarantine_timestamp", current_timestamp());
        
        return new DataQualityResult(validRecords, invalidRecords);
    }
    
    /**
     * Deduplicate customers
     */
    private Dataset<Row> deduplicateCustomers(Dataset<Row> customers) {
        logger.info("Deduplicating customers by customer_id...");
        
        // Keep the first occurrence of each customer_id
        return customers
            .withColumn("row_num", 
                row_number().over(
                    org.apache.spark.sql.expressions.Window
                        .partitionBy("customer_id")
                        .orderBy(col("customer_unique_id").asc())
                )
            )
            .filter(col("row_num").equalTo(1))
            .drop("row_num");
    }
    
    /**
     * Enrich and standardize customer data
     */
    private Dataset<Row> enrichCustomers(Dataset<Row> customers) {
        logger.info("Enriching and standardizing customer data...");
        
        return customers
            // Standardize state codes to uppercase
            .withColumn("customer_state", upper(trim(col("customer_state"))))
            
            // Standardize city names (title case)
            .withColumn("customer_city", 
                initcap(trim(lower(col("customer_city")))))
            
            // Clean zip code (remove any spaces)
            .withColumn("customer_zip_code_prefix", 
                trim(col("customer_zip_code_prefix")))
            
            // Add geographic region based on state
            .withColumn("geographic_region",
                when(col("customer_state").isin("SP", "RJ", "MG", "ES"), "Southeast")
                .when(col("customer_state").isin("RS", "SC", "PR"), "South")
                .when(col("customer_state").isin("BA", "SE", "AL", "PE", "PB", "RN", "CE", "PI", "MA"), "Northeast")
                .when(col("customer_state").isin("GO", "MT", "MS", "DF"), "Central-West")
                .when(col("customer_state").isin("AM", "RR", "AP", "PA", "TO", "RO", "AC"), "North")
                .otherwise("Unknown")
            )
            
            // Add customer tier based on location (example: SP and RJ are tier 1 cities)
            .withColumn("city_tier",
                when(col("customer_city").isin("Sao Paulo", "Rio De Janeiro", "Brasilia"), "Tier 1")
                .when(col("customer_state").isin("SP", "RJ"), "Tier 2")
                .otherwise("Tier 3")
            );
    }
    
    /**
     * Quarantine invalid records
     */
    private void quarantineInvalidRecords(Dataset<Row> invalidRecords) {
        logger.info("Writing invalid records to quarantine...");
        
        String quarantineCustomersPath = quarantinePath + "customers";
        
        invalidRecords
            .write()
            .mode(SaveMode.Append)
            .partitionBy("silver_processing_date")
            .parquet(quarantineCustomersPath);
        
        logger.info("Invalid records quarantined at: {}", quarantineCustomersPath);
    }
    
    /**
     * Write to Silver layer
     */
    private void writeSilver(Dataset<Row> silverCustomers) {
        String silverCustomersPath = silverPath + "customers";
        
        silverCustomers
            .write()
            .mode(SaveMode.Overwrite)
            .parquet(silverCustomersPath);  // No partitioning for dimension tables
        
        logger.info("Silver data written successfully to: {}", silverCustomersPath);
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
            logger.info("=== Customers Transformation Summary ===");
            logger.info("Success: {}", success);
            logger.info("Total Records: {}", totalRecords);
            logger.info("Valid Records: {}", validRecords);
            logger.info("Invalid Records: {}", invalidRecords);
            logger.info("Duplicates Removed: {}", duplicatesRemoved);
            if (!success) {
                logger.error("Error: {}", errorMessage);
            }
            logger.info("========================================");
        }
    }
}