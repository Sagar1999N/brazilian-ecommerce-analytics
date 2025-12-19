package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Transforms Products from Bronze to Silver layer.
 * 
 * Extends BaseSilverTransformer - only implements product-specific logic.
 * Clean and focused on product business rules!
 */
public class ProductsSilverTransformer extends BaseSilverTransformer {
    
    public ProductsSilverTransformer(SparkSession spark) {
        super(spark, "products");
    }
    
    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> products) {
        logger.info("Applying product-specific data quality validation rules...");
        
        Dataset<Row> productsWithValidation = products
            // Check for null primary key
            .withColumn("is_product_id_null", col("product_id").isNull())
            
            // Check for negative or zero weight
            .withColumn("is_weight_valid",
                col("product_weight_g").isNull().or(
                    col("product_weight_g").gt(0)
                )
            )
            
            // Check for valid dimensions
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
                    when(not(col("is_weight_valid")), lit("Invalid weight")).otherwise(lit("")),
                    when(not(col("is_dimensions_valid")), lit("Invalid dimensions")).otherwise(lit(""))
                )
            )
            .withColumn("quarantine_timestamp", current_timestamp());
        
        return new DataQualityResult(validRecords, invalidRecords);
    }
    
    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> products) {
        logger.info("Deduplicating products by product_id...");
        
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
    
    @Override
    protected Dataset<Row> enrichData(Dataset<Row> products) {
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
            
            // Standardize category name
            .withColumn("product_category_name",
                when(col("product_category_name").isNotNull(),
                    lower(trim(col("product_category_name")))
                ).otherwise(lit("uncategorized"))
            )
            
            // Shipping complexity score
            .withColumn("shipping_complexity_score",
                when(col("product_weight_g").isNotNull()
                    .and(col("volume_cubic_cm").isNotNull()),
                    round(
                        (col("product_weight_g").divide(1000).multiply(0.6))
                        .plus(col("volume_liters").multiply(0.4)),
                        2
                    )
                ).otherwise(lit(null))
            )
            
            // Flag for products with complete dimensions
            .withColumn("has_complete_dimensions",
                col("product_weight_g").isNotNull()
                .and(col("product_length_cm").isNotNull())
                .and(col("product_height_cm").isNotNull())
                .and(col("product_width_cm").isNotNull())
            );
    }
}