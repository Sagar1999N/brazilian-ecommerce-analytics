package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.AppConfig;

import static org.apache.spark.sql.functions.*;

/**
 * Transforms Orders from Bronze to Silver layer.
 * 
 * Responsibilities:
 * 1. Data Quality Validation
 * 2. Deduplication
 * 3. Null Handling
 * 4. Enrichment (calculated fields)
 * 5. Dead Letter Queue for bad records
 * 
 * Design Patterns Applied:
 * - Idempotency: Can run multiple times safely (using SaveMode.Overwrite)
 * - Dead Letter Queue: Bad records quarantined separately
 * - Data Quality Gates: Clear validation rules
 */
public class OrdersSilverTransformer {
    private static final Logger logger = LoggerFactory.getLogger(OrdersSilverTransformer.class);
    
    private final SparkSession spark;
    private final String bronzePath;
    private final String silverPath;
    private final String quarantinePath;
    
    public OrdersSilverTransformer(SparkSession spark) {
        this.spark = spark;
        this.bronzePath = AppConfig.getString("app.data.bronze.path", "data/staging/");
        this.silverPath = AppConfig.getString("app.data.silver.path", "data/silver/");
        this.quarantinePath = AppConfig.getString("app.data.quarantine.path", "data/quarantine/");
    }
    
    /**
     * Main transformation method - orchestrates the entire Bronze -> Silver flow
     */
    public TransformationResult transform() {
        logger.info("=== Starting Bronze -> Silver Transformation for Orders ===");
        
        TransformationResult result = new TransformationResult();
        
        try {
            // Step 1: Read Bronze data
            logger.info("Step 1: Reading Bronze data from: {}", bronzePath + "orders");
            Dataset<Row> bronzeOrders = spark.read().parquet(bronzePath + "orders");
            long totalRecords = bronzeOrders.count();
            logger.info("Bronze records read: {}", totalRecords);
            result.setTotalRecords(totalRecords);
            
            // Step 2: Add metadata columns (processing timestamp, source)
            Dataset<Row> ordersWithMetadata = addMetadataColumns(bronzeOrders);
            
            // Step 3: Data Quality Validation
            logger.info("Step 2: Applying data quality rules...");
            DataQualityResult dqResult = applyDataQualityRules(ordersWithMetadata);
            
            Dataset<Row> validOrders = dqResult.validRecords;
            Dataset<Row> invalidOrders = dqResult.invalidRecords;
            
            result.setValidRecords(validOrders.count());
            result.setInvalidRecords(invalidOrders.count());
            
            logger.info("Valid records: {}", result.getValidRecords());
            logger.info("Invalid records: {}", result.getInvalidRecords());
            
            // Step 4: Quarantine bad records (Dead Letter Queue pattern)
            if (result.getInvalidRecords() > 0) {
                logger.warn("Quarantining {} invalid records", result.getInvalidRecords());
                quarantineInvalidRecords(invalidOrders);
            }
            
            // Step 5: Deduplication
            logger.info("Step 3: Deduplicating records...");
            Dataset<Row> dedupedOrders = deduplicateOrders(validOrders);
            long afterDedup = dedupedOrders.count();
            result.setDuplicatesRemoved(result.getValidRecords() - afterDedup);
            logger.info("Duplicates removed: {}", result.getDuplicatesRemoved());
            
            // Step 6: Data Enrichment
            logger.info("Step 4: Enriching data with calculated fields...");
            Dataset<Row> enrichedOrders = enrichOrders(dedupedOrders);
            
            // Step 7: Write to Silver
            logger.info("Step 5: Writing to Silver layer: {}", silverPath + "orders");
            writeSilver(enrichedOrders);
            
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
     * Add metadata columns for tracking and auditing
     */
    private Dataset<Row> addMetadataColumns(Dataset<Row> df) {
        return df
            .withColumn("silver_processed_timestamp", current_timestamp())
            .withColumn("silver_processing_date", current_date())
            .withColumn("data_source", lit("brazilian_ecommerce"));
    }
    
    /**
     * Apply comprehensive data quality rules
     * Returns both valid and invalid records
     */
    private DataQualityResult applyDataQualityRules(Dataset<Row> orders) {
        logger.info("Applying data quality validation rules...");
        
        // Define validation rules
        // Rule 1: order_id and customer_id must not be null
        // Rule 2: order_status must be valid
        // Rule 3: purchase timestamp must not be null
        // Rule 4: Logical date validations
        
        Dataset<Row> ordersWithValidation = orders
            // Check for null primary keys
            .withColumn("is_order_id_null", col("order_id").isNull())
            .withColumn("is_customer_id_null", col("customer_id").isNull())
            
            // Check for valid order status
            .withColumn("is_status_valid", 
                col("order_status").isNotNull().and(
                    col("order_status").isin("delivered", "shipped", "canceled", 
                                            "approved", "invoiced", "processing", 
                                            "created", "unavailable")
                )
            )
            
            // Check for null timestamps
            .withColumn("is_purchase_timestamp_null", 
                col("order_purchase_timestamp").isNull())
            
            // Logical date validation: delivered date should be after purchase date
            .withColumn("is_delivery_date_logical",
                when(col("order_delivered_customer_date").isNotNull(),
                    col("order_delivered_customer_date")
                        .geq(col("order_purchase_timestamp")))
                .otherwise(lit(true))  // If delivery date is null, consider it valid
            )
            
            // Overall validation flag
			.withColumn("is_valid_record",
				not(col("is_order_id_null"))
				.and(not(col("is_customer_id_null")))
				.and(not(col("is_purchase_timestamp_null")))
				.and(col("is_status_valid"))
				.and(col("is_delivery_date_logical"))
			);
        
        // Split into valid and invalid records
        Dataset<Row> validRecords = ordersWithValidation
            .filter(col("is_valid_record").equalTo(true))
            // Drop validation columns for clean silver data
            .drop("is_order_id_null", "is_customer_id_null", "is_status_valid",
                  "is_purchase_timestamp_null", "is_delivery_date_logical", "is_valid_record");
        
        Dataset<Row> invalidRecords = ordersWithValidation
            .filter(col("is_valid_record").equalTo(false))
            .withColumn("quarantine_reason", 
                concat_ws("; ",
                    when(col("is_order_id_null"), lit("Null order_id")).otherwise(lit("")),
                    when(col("is_customer_id_null"), lit("Null customer_id")).otherwise(lit("")),
                    when(not(col("is_status_valid")), lit("Invalid order_status")).otherwise(lit("")),
                    when(col("is_purchase_timestamp_null"), lit("Null purchase_timestamp")).otherwise(lit("")),
                    when(not(col("is_delivery_date_logical")), lit("Illogical delivery date")).otherwise(lit(""))
                )
            )
            .withColumn("quarantine_timestamp", current_timestamp());
        
        return new DataQualityResult(validRecords, invalidRecords);
    }
    
    /**
     * Deduplicate orders based on order_id
     * Keep the most recent record based on purchase timestamp
     */
    private Dataset<Row> deduplicateOrders(Dataset<Row> orders) {
        logger.info("Deduplicating orders by order_id...");
        
        // Use window function to identify latest record for each order_id
        return orders
            .withColumn("row_num", 
                row_number().over(
                    org.apache.spark.sql.expressions.Window
                        .partitionBy("order_id")
                        .orderBy(col("order_purchase_timestamp").desc())
                )
            )
            .filter(col("row_num").equalTo(1))
            .drop("row_num");
    }
    
    /**
     * Enrich orders with calculated fields
     * - Delivery time in days
     * - Order processing time
     * - Is order late?
     */
    private Dataset<Row> enrichOrders(Dataset<Row> orders) {
        logger.info("Enriching orders with calculated fields...");
        
        return orders
            // Calculate actual delivery time in days
            .withColumn("delivery_time_days",
                when(col("order_delivered_customer_date").isNotNull(),
                    datediff(col("order_delivered_customer_date"), 
                            col("order_purchase_timestamp")))
                .otherwise(lit(null))
            )
            
            // Calculate expected delivery time
            .withColumn("expected_delivery_time_days",
                when(col("order_estimated_delivery_date").isNotNull(),
                    datediff(col("order_estimated_delivery_date"), 
                            col("order_purchase_timestamp")))
                .otherwise(lit(null))
            )
            
            // Is order delivered late?
            .withColumn("is_late_delivery",
                when(col("order_delivered_customer_date").isNotNull()
                    .and(col("order_estimated_delivery_date").isNotNull()),
                    col("order_delivered_customer_date")
                        .gt(col("order_estimated_delivery_date")))
                .otherwise(lit(null))
            )
            
            // Order approval time (time from purchase to approval)
            .withColumn("approval_time_hours",
                when(col("order_approved_at").isNotNull(),
                    round(
                        (unix_timestamp(col("order_approved_at"))
                         .minus(unix_timestamp(col("order_purchase_timestamp"))))
                         .divide(3600), 2
                    ))
                .otherwise(lit(null))
            )
            
            // Extract date parts for analytics
            .withColumn("order_year", year(col("order_purchase_timestamp")))
            .withColumn("order_month", month(col("order_purchase_timestamp")))
            .withColumn("order_day", dayofmonth(col("order_purchase_timestamp")))
            .withColumn("order_day_of_week", dayofweek(col("order_purchase_timestamp")))
            .withColumn("order_quarter", quarter(col("order_purchase_timestamp")));
    }
    
    /**
     * Quarantine invalid records to Dead Letter Queue
     */
    private void quarantineInvalidRecords(Dataset<Row> invalidRecords) {
        logger.info("Writing invalid records to quarantine...");
        
        String quarantineOrdersPath = quarantinePath + "orders";
        
        invalidRecords
            .write()
            .mode(SaveMode.Append)  // Append to track all quarantined records over time
            .partitionBy("silver_processing_date")  // Partition by processing date
            .parquet(quarantineOrdersPath);
        
        logger.info("Invalid records quarantined at: {}", quarantineOrdersPath);
    }
    
    /**
     * Write to Silver layer with partitioning
     */
    private void writeSilver(Dataset<Row> silverOrders) {
        String silverOrdersPath = silverPath + "orders";
        
        silverOrders
            .write()
            .mode(SaveMode.Overwrite)  // Idempotent - can rerun safely
            .partitionBy("order_year", "order_month")  // Partition for query performance
            .parquet(silverOrdersPath);
        
        logger.info("Silver data written successfully to: {}", silverOrdersPath);
    }
    
    // ========== Helper Classes ==========
    
    private static class DataQualityResult {
        Dataset<Row> validRecords;
        Dataset<Row> invalidRecords;
        
        DataQualityResult(Dataset<Row> validRecords, Dataset<Row> invalidRecords) {
            this.validRecords = validRecords;
            this.invalidRecords = invalidRecords;
        }
    }
    
    /**
     * Result object to track transformation metrics
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
        
        public void logSummary(Logger logger) {
            logger.info("=== Transformation Summary ===");
            logger.info("Success: {}", success);
            logger.info("Total Records: {}", totalRecords);
            logger.info("Valid Records: {}", validRecords);
            logger.info("Invalid Records: {}", invalidRecords);
            logger.info("Duplicates Removed: {}", duplicatesRemoved);
            if (!success) {
                logger.error("Error: {}", errorMessage);
            }
            logger.info("==============================");
        }
    }
}