package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Transforms Customers from Bronze to Silver layer.
 * 
 * Extends BaseSilverTransformer - only implements customer-specific logic.
 * Much cleaner than the original version!
 */
public class CustomersSilverTransformer extends BaseSilverTransformer {
    
    public CustomersSilverTransformer(SparkSession spark) {
        super(spark, "customers");
    }
    
    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> customers) {
        logger.info("Applying customer-specific data quality validation rules...");
        
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
            
            // Check for valid zip code
            .withColumn("is_zip_valid",
                col("customer_zip_code_prefix").isNotNull().and(
                    length(col("customer_zip_code_prefix")).geq(5)
                )
            )
            
            // Check for valid city name
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
    
    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> customers) {
        logger.info("Deduplicating customers by customer_id...");
        
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
    
    @Override
    protected Dataset<Row> enrichData(Dataset<Row> customers) {
        logger.info("Enriching and standardizing customer data...");
        
        return customers
            // Standardize state codes to uppercase
            .withColumn("customer_state", upper(trim(col("customer_state"))))
            
            // Standardize city names (title case)
            .withColumn("customer_city", 
                initcap(trim(lower(col("customer_city")))))
            
            // Clean zip code
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
            
            // Add customer tier based on location
            .withColumn("city_tier",
                when(col("customer_city").isin("Sao Paulo", "Rio De Janeiro", "Brasilia"), "Tier 1")
                .when(col("customer_state").isin("SP", "RJ"), "Tier 2")
                .otherwise("Tier 3")
            );
    }
}