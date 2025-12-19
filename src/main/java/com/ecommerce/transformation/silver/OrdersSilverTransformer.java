package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Transforms Orders from Bronze to Silver layer.
 * 
 * Extends BaseSilverTransformer to leverage common transformation logic.
 * Implements only entity-specific logic:
 * - Order-specific data quality rules
 * - Order-specific deduplication strategy
 * - Order-specific enrichment calculations
 * 
 * This class is much cleaner thanks to inheritance!
 */
public class OrdersSilverTransformer extends BaseSilverTransformer {
    
    public OrdersSilverTransformer(SparkSession spark) {
        super(spark, "orders");  // Pass entity name to base class
    }
    
    /**
     * Apply order-specific data quality rules.
     */
    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> orders) {
        logger.info("Applying order-specific data quality validation rules...");
        
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
                .otherwise(lit(true))
            )
            
            // Overall validation flag
            .withColumn("is_valid_record",
                not(col("is_order_id_null"))
                .and(not(col("is_customer_id_null")))
                .and(col("is_status_valid"))
                .and(not(col("is_purchase_timestamp_null")))
                .and(col("is_delivery_date_logical"))
            );
        
        // Split into valid and invalid records
        Dataset<Row> validRecords = ordersWithValidation
            .filter(col("is_valid_record").equalTo(true))
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
     * Deduplicate orders based on order_id.
     * Keep the most recent record based on purchase timestamp.
     */
    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> orders) {
        logger.info("Deduplicating orders by order_id...");
        
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
     * Enrich orders with calculated fields.
     */
    @Override
    protected Dataset<Row> enrichData(Dataset<Row> orders) {
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
     * Override writeSilver to add partitioning for orders.
     * Orders benefit from time-based partitioning due to large volume.
     */
    @Override
    protected void writeSilver(Dataset<Row> silverOrders) {
        String silverOrdersPath = silverPath + entityName;
        
        silverOrders
            .write()
            .mode(SaveMode.Overwrite)
            .partitionBy("order_year", "order_month")  // Time-based partitioning
            .parquet(silverOrdersPath);
        
        logger.info("Silver data written successfully to: {}", silverOrdersPath);
    }
}