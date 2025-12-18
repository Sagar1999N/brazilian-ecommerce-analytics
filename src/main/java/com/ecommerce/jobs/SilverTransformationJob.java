package com.ecommerce.jobs;

import java.io.File;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.SparkSessionFactory;
import com.ecommerce.transformation.silver.OrdersSilverTransformer;
import com.ecommerce.transformation.silver.CustomersSilverTransformer;
import com.ecommerce.transformation.silver.ProductsSilverTransformer;

/**
 * Job to transform Bronze layer data to Silver layer for ALL entities.
 * 
 * This job:
 * 1. Transforms Orders (Bronze → Silver)
 * 2. Transforms Customers (Bronze → Silver)
 * 3. Transforms Products (Bronze → Silver)
 * 
 * Each transformation includes:
 * - Data quality rules and validation
 * - Deduplication logic
 * - Data enrichment with calculated fields
 * - Quarantine logic for invalid records (DLQ pattern)
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
        logger.info("║         Transforming: Orders, Customers, Products             ║");
        logger.info("╚═══════════════════════════════════════════════════════════════╝");
        
        SparkSession spark = null;
        long startTime = System.currentTimeMillis();
        boolean allSuccess = true;
        
        try {
            // Step 1: Initialize Spark Session
            logger.info("Step 1: Initializing Spark Session...");
            spark = SparkSessionFactory.createSession("silver-transformation");
            
            // Step 2: Transform Orders
            logger.info("");
            logger.info("═══════════════════════════════════════════════════════════════");
            logger.info("Step 2: Transforming ORDERS (Bronze → Silver)");
            logger.info("═══════════════════════════════════════════════════════════════");
            OrdersSilverTransformer ordersTransformer = new OrdersSilverTransformer(spark);
            OrdersSilverTransformer.TransformationResult ordersResult = ordersTransformer.transform();
            ordersResult.logSummary(logger);
            
            if (!ordersResult.isSuccess()) {
                logger.error("❌ Orders transformation FAILED!");
                allSuccess = false;
            } else {
                logger.info("✓ Orders transformation completed successfully");
            }
            
            // Step 3: Transform Customers
            logger.info("");
            logger.info("═══════════════════════════════════════════════════════════════");
            logger.info("Step 3: Transforming CUSTOMERS (Bronze → Silver)");
            logger.info("═══════════════════════════════════════════════════════════════");
            CustomersSilverTransformer customersTransformer = new CustomersSilverTransformer(spark);
            CustomersSilverTransformer.TransformationResult customersResult = customersTransformer.transform();
            customersResult.logSummary(logger);
            
            if (!customersResult.isSuccess()) {
                logger.error("❌ Customers transformation FAILED!");
                allSuccess = false;
            } else {
                logger.info("✓ Customers transformation completed successfully");
            }
            
            // Step 4: Transform Products
            logger.info("");
            logger.info("═══════════════════════════════════════════════════════════════");
            logger.info("Step 4: Transforming PRODUCTS (Bronze → Silver)");
            logger.info("═══════════════════════════════════════════════════════════════");
            ProductsSilverTransformer productsTransformer = new ProductsSilverTransformer(spark);
            ProductsSilverTransformer.TransformationResult productsResult = productsTransformer.transform();
            productsResult.logSummary(logger);
            
            if (!productsResult.isSuccess()) {
                logger.error("❌ Products transformation FAILED!");
                allSuccess = false;
            } else {
                logger.info("✓ Products transformation completed successfully");
            }
            
            // Step 5: Final Summary
            logger.info("");
            logger.info("╔═══════════════════════════════════════════════════════════════╗");
            logger.info("║              SILVER TRANSFORMATION SUMMARY                    ║");
            logger.info("╠═══════════════════════════════════════════════════════════════╣");
            logger.info("║  ORDERS:                                                      ║");
            logger.info("║    Total: {} | Valid: {} | Invalid: {}", 
                String.format("%-8d", ordersResult.getTotalRecords()),
                String.format("%-8d", ordersResult.getValidRecords()),
                String.format("%-8d", ordersResult.getInvalidRecords()));
            logger.info("║                                                               ║");
            logger.info("║  CUSTOMERS:                                                   ║");
            logger.info("║    Total: {} | Valid: {} | Invalid: {}", 
                String.format("%-8d", customersResult.getTotalRecords()),
                String.format("%-8d", customersResult.getValidRecords()),
                String.format("%-8d", customersResult.getInvalidRecords()));
            logger.info("║                                                               ║");
            logger.info("║  PRODUCTS:                                                    ║");
            logger.info("║    Total: {} | Valid: {} | Invalid: {}", 
                String.format("%-8d", productsResult.getTotalRecords()),
                String.format("%-8d", productsResult.getValidRecords()),
                String.format("%-8d", productsResult.getInvalidRecords()));
            logger.info("╚═══════════════════════════════════════════════════════════════╝");
            
            // Check overall success
            if (!allSuccess) {
                logger.error("❌ Some transformations FAILED! Check logs above.");
                System.exit(1);
            }
            
            // Calculate and log job duration
            long endTime = System.currentTimeMillis();
            long durationSeconds = (endTime - startTime) / 1000;
            
            logger.info("");
            logger.info("╔═══════════════════════════════════════════════════════════════╗");
            logger.info("║         SILVER TRANSFORMATION JOB - COMPLETED                 ║");
            logger.info("║         Duration: {} seconds", String.format("%-38s", durationSeconds));
            logger.info("║         Status: SUCCESS                                        ║");
            logger.info("╚═══════════════════════════════════════════════════════════════╝");
            logger.info("");
            logger.info("✓ Silver layer is ready!");
            logger.info("  → data/silver/orders/");
            logger.info("  → data/silver/customers/");
            logger.info("  → data/silver/products/");
            logger.info("");
            logger.info("Next step: Run GoldTransformationJob to create dimensional model");
            
        } catch (Exception e) {
            logger.error("╔═══════════════════════════════════════════════════════════════╗");
            logger.error("║         SILVER TRANSFORMATION JOB - FAILED                    ║");
            logger.error("╚═══════════════════════════════════════════════════════════════╝");
            logger.error("Fatal error during Silver transformation", e);
            
            logger.error("Exception type: {}", e.getClass().getName());
            logger.error("Exception message: {}", e.getMessage());
            
            if (e.getCause() != null) {
                logger.error("Caused by: {}", e.getCause().getMessage());
            }
            
            System.exit(1);
            
        } finally {
            if (spark != null) {
                logger.info("Cleaning up Spark session...");
                SparkSessionFactory.stopSession(spark);
            }
        }
    }
}