package com.ecommerce.ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Validates data quality and schema compliance
 */
public class DataValidator {
    private static final Logger logger = LoggerFactory.getLogger(DataValidator.class);
    
    /**
     * Validates dataset against expected schema
     */
    public ValidationResult validateSchema(Dataset<Row> dataset, StructType expectedSchema) {
        ValidationResult result = new ValidationResult();
        StructType actualSchema = dataset.schema();
        
        logger.info("Validating schema...");
        logger.debug("Expected: {}", expectedSchema.treeString());
        logger.debug("Actual: {}", actualSchema.treeString());
        
        // Check field count
        if (expectedSchema.fields().length != actualSchema.fields().length) {
            result.addError("Schema field count mismatch. Expected: " + 
                          expectedSchema.fields().length + ", Actual: " + 
                          actualSchema.fields().length);
        }
        
        // Check individual fields
        for (int i = 0; i < Math.min(expectedSchema.fields().length, actualSchema.fields().length); i++) {
            var expectedField = expectedSchema.fields()[i];
            var actualField = actualSchema.fields()[i];
            
            if (!expectedField.name().equals(actualField.name())) {
                result.addError("Field name mismatch at position " + i + 
                              ". Expected: " + expectedField.name() + 
                              ", Actual: " + actualField.name());
            }
            
            if (!expectedField.dataType().equals(actualField.dataType())) {
                result.addWarning("Field type mismatch for '" + expectedField.name() + 
                                "'. Expected: " + expectedField.dataType() + 
                                ", Actual: " + actualField.dataType());
            }
            
            if (expectedField.nullable() != actualField.nullable()) {
                result.addWarning("Nullability mismatch for '" + expectedField.name() + 
                                "'. Expected nullable: " + expectedField.nullable() + 
                                ", Actual nullable: " + actualField.nullable());
            }
        }
        
        return result;
    }
    
    /**
     * Validates data quality rules
     */
    public ValidationResult validateDataQuality(Dataset<Row> dataset, String dataType) {
        ValidationResult result = new ValidationResult();
        long totalCount = dataset.count();
        
        logger.info("Validating data quality for {} ({} rows)", dataType, totalCount);
        
        // Check for null primary keys
        switch (dataType.toLowerCase()) {
            case "orders":
                long nullOrderIds = dataset.filter(dataset.col("order_id").isNull()).count();
                if (nullOrderIds > 0) {
                    result.addError("Found " + nullOrderIds + " rows with null order_id");
                }
                break;
                
            case "customers":
                long nullCustomerIds = dataset.filter(dataset.col("customer_id").isNull()).count();
                if (nullCustomerIds > 0) {
                    result.addError("Found " + nullCustomerIds + " rows with null customer_id");
                }
                break;
                
            case "products":
                long nullProductIds = dataset.filter(dataset.col("product_id").isNull()).count();
                if (nullProductIds > 0) {
                    result.addError("Found " + nullProductIds + " rows with null product_id");
                }
                break;
        }
        
        // Check for duplicates
        if (dataType.equals("orders")) {
            long distinctCount = dataset.select("order_id").distinct().count();
            if (distinctCount < totalCount) {
                long duplicates = totalCount - distinctCount;
                result.addError("Found " + duplicates + " duplicate order_ids");
            }
        }
        
        // Check for negative values where applicable
        if (dataType.equals("products")) {
            long negativeWeight = dataset.filter(dataset.col("product_weight_g").lt(0)).count();
            if (negativeWeight > 0) {
                result.addError("Found " + negativeWeight + " products with negative weight");
            }
        }
        
        return result;
    }
    
    /**
     * Validation result container
     */
    public static class ValidationResult {
        private final Map<String, Integer> errors = new HashMap<>();
        private final Map<String, Integer> warnings = new HashMap<>();
        
        public void addError(String error) {
            errors.put(error, errors.getOrDefault(error, 0) + 1);
        }
        
        public void addWarning(String warning) {
            warnings.put(warning, warnings.getOrDefault(warning, 0) + 1);
        }
        
        public boolean hasErrors() {
            return !errors.isEmpty();
        }
        
        public boolean hasWarnings() {
            return !warnings.isEmpty();
        }
        
        public void logResults() {
            if (hasErrors()) {
                logger.error("Validation found {} error(s):", errors.size());
                errors.forEach((error, count) -> 
                    logger.error("  {} ({} occurrences)", error, count));
            } else {
                logger.info("No validation errors found");
            }
            
            if (hasWarnings()) {
                logger.warn("Validation found {} warning(s):", warnings.size());
                warnings.forEach((warning, count) -> 
                    logger.warn("  {} ({} occurrences)", warning, count));
            }
        }
    }
}