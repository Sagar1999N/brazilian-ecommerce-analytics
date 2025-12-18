package com.ecommerce.jobs;

import java.io.File;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.SparkSessionFactory;
import com.ecommerce.transformation.silver.OrdersSilverTransformer;

/**
 * Job to transform Bronze layer data to Silver layer.
 * 
 * This job:
 * 1. Reads validated data from Bronze (staging)
 * 2. Applies data quality rules
 * 3. Deduplicates records
 * 4. Enriches with calculated fields
 * 5. Quarantines invalid records
 * 6. Writes clean data to Silver layer
 * 
 * Design Patterns:
 * - Idempotent: Can be re-run safely
 * - Fail-fast: Exits with error code on failure
 * - Observable: Comprehensive logging and metrics
 */
public class SilverTransformationJob {
    private static Logger logger;
    
    static {
        System.out.println("=== CONFIGURING LOG4J ===");
        new File("logs").mkdirs();
        System.setProperty("log4j.configurationFile", "src/main/resources/log4j2.xml");
        System.setProperty("APP_LOG_ROOT", "logs");
        Configurator.reconfigure();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger = LoggerFactory.getLogger(SilverTransformationJob.class);
    }
    
    public static void main(String[] args) {
        logger.info("╔═══════════════════════════════════════════════════════════════╗");
        logger.info("║         SILVER TRANSFORMATION JOB - STARTED                   ║");
        logger.info("╚═══════════════════════════════════════════════════════════════╝");
        
        SparkSession spark = null;
        long startTime = System.currentTimeMillis();
        
        try {
            // Step 1: Initialize Spark Session
            logger.info("Step 1: Initializing Spark Session...");
            spark = SparkSessionFactory.createSession("silver-transformation");
            
            // Step 2: Transform Orders from Bronze to Silver
            logger.info("Step 2: Starting Orders Bronze → Silver transformation...");
            OrdersSilverTransformer ordersTransformer = new OrdersSilverTransformer(spark);
            OrdersSilverTransformer.TransformationResult ordersResult = ordersTransformer.transform();
            
            // Step 3: Log Results
            logger.info("Step 3: Logging transformation results...");
            ordersResult.logSummary(logger);
            
            // Step 4: Validate Success
            if (!ordersResult.isSuccess()) {
                logger.error("❌ Orders transformation FAILED!");
                logger.error("Error: {}", ordersResult.getErrorMessage());
                System.exit(1);
            }
            
            // Step 5: Calculate and log job duration
            long endTime = System.currentTimeMillis();
            long durationSeconds = (endTime - startTime) / 1000;
            
            logger.info("╔═══════════════════════════════════════════════════════════════╗");
            logger.info("║         SILVER TRANSFORMATION JOB - COMPLETED                 ║");
            logger.info("║         Duration: {} seconds", String.format("%-38s", durationSeconds));
            logger.info("║         Status: SUCCESS                                        ║");
            logger.info("╚═══════════════════════════════════════════════════════════════╝");
            
            // Future: Add Customers and Products transformations here
            // CustomersSilverTransformer customersTransformer = new CustomersSilverTransformer(spark);
            // ProductsSilverTransformer productsTransformer = new ProductsSilverTransformer(spark);
            
        } catch (Exception e) {
            logger.error("╔═══════════════════════════════════════════════════════════════╗");
            logger.error("║         SILVER TRANSFORMATION JOB - FAILED                    ║");
            logger.error("╚═══════════════════════════════════════════════════════════════╝");
            logger.error("Fatal error during Silver transformation", e);
            
            // Print exception details for debugging
            logger.error("Exception type: {}", e.getClass().getName());
            logger.error("Exception message: {}", e.getMessage());
            
            if (e.getCause() != null) {
                logger.error("Caused by: {}", e.getCause().getMessage());
            }
            
            System.exit(1);
            
        } finally {
            // Always cleanup Spark session
            if (spark != null) {
                logger.info("Cleaning up Spark session...");
                SparkSessionFactory.stopSession(spark);
            }
        }
    }
}