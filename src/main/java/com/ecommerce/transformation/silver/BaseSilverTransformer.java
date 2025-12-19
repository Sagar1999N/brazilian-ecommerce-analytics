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
 * Abstract base class for Silver layer transformers.
 * 
 * Implements Template Method pattern to provide common transformation logic
 * while allowing subclasses to customize specific steps.
 * 
 * Common responsibilities handled by base class:
 * - Path configuration
 * - Metadata addition
 * - Quarantine logic
 * - Silver write operations
 * - Result tracking
 * 
 * Subclasses must implement:
 * - Entity-specific data quality rules
 * - Deduplication logic
 * - Enrichment logic
 * 
 * Design Patterns:
 * - Template Method: Defines algorithm skeleton
 * - DRY (Don't Repeat Yourself): Eliminates code duplication
 * - Single Responsibility: Each method has one clear purpose
 */
public abstract class BaseSilverTransformer {
    protected final Logger logger;
    protected final SparkSession spark;
    protected final String bronzePath;
    protected final String silverPath;
    protected final String quarantinePath;
    protected final String entityName;  // e.g., "orders", "customers", "products"
    
    /**
     * Constructor - initializes common fields
     * 
     * @param spark SparkSession
     * @param entityName Name of the entity being transformed (e.g., "orders")
     */
    protected BaseSilverTransformer(SparkSession spark, String entityName) {
        this.logger = LoggerFactory.getLogger(this.getClass());
        this.spark = spark;
        this.entityName = entityName;
        this.bronzePath = AppConfig.getString("app.data.bronze.path", "data/bronze/");
        this.silverPath = AppConfig.getString("app.data.silver.path", "data/silver/");
        this.quarantinePath = AppConfig.getString("app.data.quarantine.path", "data/quarantine/");
    }
    
    /**
     * Template Method - Defines the transformation algorithm.
     * This is the main method that orchestrates the entire transformation.
     * 
     * Subclasses should NOT override this method.
     */
    public final TransformationResult transform() {
        logger.info("=== Starting Bronze -> Silver Transformation for {} ===", entityName);
        
        TransformationResult result = new TransformationResult();
        
        try {
            // Step 1: Read Bronze data
            logger.info("Step 1: Reading Bronze data from: {}", bronzePath + entityName);
            Dataset<Row> bronzeData = readBronzeData();
            long totalRecords = bronzeData.count();
            logger.info("Bronze records read: {}", totalRecords);
            result.setTotalRecords(totalRecords);
            
            // Step 2: Add metadata columns (common to all entities)
            Dataset<Row> dataWithMetadata = addMetadataColumns(bronzeData);
            
            // Step 3: Apply data quality rules (entity-specific)
            logger.info("Step 2: Applying data quality rules...");
            DataQualityResult dqResult = applyDataQualityRules(dataWithMetadata);
            
            Dataset<Row> validRecords = dqResult.validRecords;
            Dataset<Row> invalidRecords = dqResult.invalidRecords;

            result.setValidRecords(validRecords.count());
            result.setInvalidRecords(invalidRecords.count());
            
            logger.info("Valid records: {}", result.getValidRecords());
            logger.info("Invalid records: {}", result.getInvalidRecords());
            
            // Step 4: Quarantine bad records (common logic)
            if (result.getInvalidRecords() > 0) {
                logger.warn("Quarantining {} invalid records", result.getInvalidRecords());
                quarantineInvalidRecords(invalidRecords);
            }
            
            // Step 5: Deduplication (entity-specific)
            logger.info("Step 3: Deduplicating records...");
            Dataset<Row> dedupedData = deduplicateRecords(validRecords);
            long afterDedup = dedupedData.count();
            result.setDuplicatesRemoved(result.getValidRecords() - afterDedup);
            logger.info("Duplicates removed: {}", result.getDuplicatesRemoved());
            
            // Step 6: Data Enrichment (entity-specific)
            logger.info("Step 4: Enriching data with calculated fields...");
            Dataset<Row> enrichedData = enrichData(dedupedData);
            
            // Step 7: Write to Silver (common logic)
            logger.info("Step 5: Writing to Silver layer: {}", silverPath + entityName);
            writeSilver(enrichedData);
            
            logger.info("=== Bronze -> Silver Transformation Completed Successfully ===");
            result.setSuccess(true);
            
        } catch (Exception e) {
            logger.error("Transformation failed for {}", entityName, e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Reads Bronze data for this entity.
     * Can be overridden if custom read logic is needed.
     */
    protected Dataset<Row> readBronzeData() {
        return spark.read().parquet(bronzePath + entityName);
    }
    
    /**
     * Adds common metadata columns to all records.
     * This method is FINAL - cannot be overridden.
     */
    protected final Dataset<Row> addMetadataColumns(Dataset<Row> df) {
        return df
            .withColumn("silver_processed_timestamp", current_timestamp())
            .withColumn("silver_processing_date", current_date())
            .withColumn("data_source", lit("brazilian_ecommerce"));
    }
    
    /**
     * Quarantines invalid records with common logic.
     * This method is FINAL - cannot be overridden.
     */
    protected final void quarantineInvalidRecords(Dataset<Row> invalidRecords) {
        logger.info("Writing invalid records to quarantine...");
        
        String quarantinePath = this.quarantinePath + entityName;
        
        invalidRecords
            .write()
            .mode(SaveMode.Append)
            .partitionBy("silver_processing_date")
            .parquet(quarantinePath);
        
        logger.info("Invalid records quarantined at: {}", quarantinePath);
    }
    
    /**
     * Writes to Silver layer with common logic.
     * Can be overridden if custom write logic is needed (e.g., partitioning).
     */
    protected void writeSilver(Dataset<Row> silverData) {
        String silverDataPath = silverPath + entityName;
        
        silverData
            .write()
            .mode(SaveMode.Overwrite)
            .parquet(silverDataPath);
        
        logger.info("Silver data written successfully to: {}", silverDataPath);
    }
    
    // ========== ABSTRACT METHODS - Must be implemented by subclasses ==========
    
    /**
     * Applies entity-specific data quality rules.
     * Each entity has different validation requirements.
     * 
     * @param data Input data with metadata
     * @return DataQualityResult containing valid and invalid records
     */
    protected abstract DataQualityResult applyDataQualityRules(Dataset<Row> data);
    
    /**
     * Implements entity-specific deduplication logic.
     * Different entities may have different deduplication strategies.
     * 
     * @param data Valid data after quality checks
     * @return Deduplicated data
     */
    protected abstract Dataset<Row> deduplicateRecords(Dataset<Row> data);
    
    /**
     * Enriches data with entity-specific calculated fields.
     * Each entity may have different enrichment logic.
     * 
     * @param data Deduplicated data
     * @return Enriched data
     */
    protected abstract Dataset<Row> enrichData(Dataset<Row> data);
    
    // ========== HELPER CLASSES ==========
    
    /**
     * Container for data quality validation results.
     * Separates valid and invalid records.
     */
    protected static class DataQualityResult {
        public final Dataset<Row> validRecords;
        public final Dataset<Row> invalidRecords;
        
        public DataQualityResult(Dataset<Row> validRecords, Dataset<Row> invalidRecords) {
            this.validRecords = validRecords;
            this.invalidRecords = invalidRecords;
        }
    }
    
    /**
     * Result object to track transformation metrics.
     * Provides comprehensive statistics about the transformation.
     */
    public static class TransformationResult {
        private boolean success;
        private long totalRecords;
        private long validRecords;
        private long invalidRecords;
        private long duplicatesRemoved;
        private String errorMessage;
        
        // Getters and setters
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
        
        /**
         * Logs a formatted summary of the transformation results.
         * 
         * @param logger Logger to use for output
         * @param entityName Name of entity for the summary
         */
        public void logSummary(Logger logger, String entityName) {
            logger.info("=== {} Transformation Summary ===", 
                entityName.substring(0, 1).toUpperCase() + entityName.substring(1));
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