package com.ecommerce.jobs;

import java.io.File;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.SparkSessionFactory;
import com.ecommerce.transformation.silver.OrdersSilverTransformer;
import com.ecommerce.transformation.silver.ProductCategoryTranslationSilverTransformer;
import com.ecommerce.transformation.silver.BaseSilverTransformer;
import com.ecommerce.transformation.silver.CustomersSilverTransformer;
import com.ecommerce.transformation.silver.GeolocationSilverTransformer;
import com.ecommerce.transformation.silver.OrderItemsSilverTransformer;
import com.ecommerce.transformation.silver.OrderPaymentsSilverTransformer;
import com.ecommerce.transformation.silver.OrderReviewsSilverTransformer;
import com.ecommerce.transformation.silver.ProductsSilverTransformer;
import com.ecommerce.transformation.silver.SellersSilverTransformer;

/**
 * Job to transform Bronze layer data to Silver layer for ALL entities.
 * 
 * This job: 1. Transforms Orders (Bronze → Silver) 2. Transforms Customers
 * (Bronze → Silver) 3. Transforms Products (Bronze → Silver)
 * 
 * Each transformation includes: - Data quality rules and validation -
 * Deduplication logic - Data enrichment with calculated fields - Quarantine
 * logic for invalid records (DLQ pattern)
 * 
 * Design Patterns: - Idempotent: Can be re-run safely - Fail-fast: Exits with
 * error code on failure - Observable: Comprehensive logging and metrics
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
			BaseSilverTransformer.TransformationResult ordersResult = ordersTransformer.transform();
			ordersResult.logSummary(logger, "orders");

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
			BaseSilverTransformer.TransformationResult customersResult = customersTransformer.transform();
			customersResult.logSummary(logger, "customers");

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
			BaseSilverTransformer.TransformationResult productsResult = productsTransformer.transform();
			productsResult.logSummary(logger, "products");

			if (!productsResult.isSuccess()) {
				logger.error("❌ Products transformation FAILED!");
				allSuccess = false;
			} else {
				logger.info("✓ Products transformation completed successfully");
			}

			// After products, add:
			logger.info("");
			logger.info("═══════════════════════════════════════════════════════════════");
			logger.info("Step 5: Transforming ORDER ITEMS (Bronze → Silver)");
			logger.info("═══════════════════════════════════════════════════════════════");
			OrderItemsSilverTransformer orderItemsTransformer = new OrderItemsSilverTransformer(spark);
			BaseSilverTransformer.TransformationResult orderItemsResult = orderItemsTransformer.transform();
			orderItemsResult.logSummary(logger, "order_items");

			if (!orderItemsResult.isSuccess()) {
				logger.error("❌ Order Items transformation FAILED!");
				allSuccess = false;
			} else {
				logger.info("✓ Order Items transformation completed successfully");
			}

			// Add after order_items block
			logger.info("");
			logger.info("═══════════════════════════════════════════════════════════════");
			logger.info("Step 6: Transforming ORDER PAYMENTS (Bronze → Silver)");
			logger.info("═══════════════════════════════════════════════════════════════");
			OrderPaymentsSilverTransformer orderPaymentsTransformer = new OrderPaymentsSilverTransformer(spark);
			BaseSilverTransformer.TransformationResult orderPaymentsResult = orderPaymentsTransformer.transform();
			orderPaymentsResult.logSummary(logger, "order_payments");

			if (!orderPaymentsResult.isSuccess()) {
				logger.error("❌ Order Payments transformation FAILED!");
				allSuccess = false;
			} else {
				logger.info("✓ Order Payments transformation completed successfully");
			}

			// After order_payments
			logger.info("");
			logger.info("═══════════════════════════════════════════════════════════════");
			logger.info("Step 7: Transforming ORDER REVIEWS (Bronze → Silver)");
			logger.info("═══════════════════════════════════════════════════════════════");
			OrderReviewsSilverTransformer orderReviewsTransformer = new OrderReviewsSilverTransformer(spark);
			BaseSilverTransformer.TransformationResult orderReviewsResult = orderReviewsTransformer.transform();
			orderReviewsResult.logSummary(logger, "order_reviews");

			if (!orderReviewsResult.isSuccess()) {
				logger.error("❌ Order Reviews transformation FAILED!");
				allSuccess = false;
			} else {
				logger.info("✓ Order Reviews transformation completed successfully");
			}

			// After order_reviews
			logger.info("");
			logger.info("═══════════════════════════════════════════════════════════════");
			logger.info("Step 8: Transforming SELLERS (Bronze → Silver)");
			logger.info("═══════════════════════════════════════════════════════════════");
			SellersSilverTransformer sellersTransformer = new SellersSilverTransformer(spark);
			BaseSilverTransformer.TransformationResult sellersResult = sellersTransformer.transform();
			sellersResult.logSummary(logger, "sellers");

			if (!sellersResult.isSuccess()) {
				logger.error("❌ Sellers transformation FAILED!");
				allSuccess = false;
			} else {
				logger.info("✓ Sellers transformation completed successfully");
			}

			// After sellers
			logger.info("");
			logger.info("═══════════════════════════════════════════════════════════════");
			logger.info("Step 9: Transforming PRODUCT CATEGORY TRANSLATION (Bronze → Silver)");
			logger.info("═══════════════════════════════════════════════════════════════");
			ProductCategoryTranslationSilverTransformer translationTransformer = new ProductCategoryTranslationSilverTransformer(
					spark);
			BaseSilverTransformer.TransformationResult translationResult = translationTransformer.transform();
			translationResult.logSummary(logger, "product_category_name_translation");

			if (!translationResult.isSuccess()) {
				logger.error("❌ Product Category Translation transformation FAILED!");
				allSuccess = false;
			} else {
				logger.info("✓ Product Category Translation transformation completed successfully");
			}
			
			// Step 10: Transforming GEOLOCATION (Bronze → Silver)
			logger.info("");
			logger.info("═══════════════════════════════════════════════════════════════");
			logger.info("Step 10: Transforming GEOLOCATION (Bronze → Silver)");
			logger.info("═══════════════════════════════════════════════════════════════");
			GeolocationSilverTransformer geolocationTransformer = new GeolocationSilverTransformer(spark);
			BaseSilverTransformer.TransformationResult geolocationResult = geolocationTransformer.transform();
			geolocationResult.logSummary(logger, "geolocation");

			if (!geolocationResult.isSuccess()) {
			    logger.error("❌ Geolocation transformation FAILED!");
			    allSuccess = false;
			} else {
			    logger.info("✓ Geolocation transformation completed successfully");
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
			logger.info("║                                                               ║");
			logger.info("║  ORDER_ITEMS:                                                 ║");
			logger.info("║    Total: {} | Valid: {} | Invalid: {}",
					String.format("%-8d", orderItemsResult.getTotalRecords()),
					String.format("%-8d", orderItemsResult.getValidRecords()),
					String.format("%-8d", orderItemsResult.getInvalidRecords()));
			logger.info("║                                                               ║");
			logger.info("║  ORDER_PAYMENTS:                                              ║");
			logger.info("║    Total: {} | Valid: {} | Invalid: {}",
					String.format("%-8d", orderPaymentsResult.getTotalRecords()),
					String.format("%-8d", orderPaymentsResult.getValidRecords()),
					String.format("%-8d", orderPaymentsResult.getInvalidRecords()));
			logger.info("║                                                               ║");
			logger.info("║  ORDER_REVIEWS:                                               ║");
			logger.info("║    Total: {} | Valid: {} | Invalid: {}",
					String.format("%-8d", orderReviewsResult.getTotalRecords()),
					String.format("%-8d", orderReviewsResult.getValidRecords()),
					String.format("%-8d", orderReviewsResult.getInvalidRecords()));
			logger.info("║                                                               ║");
			logger.info("║  SELLERS:                                                     ║");
			logger.info("║    Total: {} | Valid: {} | Invalid: {}",
					String.format("%-8d", sellersResult.getTotalRecords()),
					String.format("%-8d", sellersResult.getValidRecords()),
					String.format("%-8d", sellersResult.getInvalidRecords()));
			logger.info("║                                                               ║");
			logger.info("║  CATEGORY_TRANSLATION:                                        ║");
			logger.info("║    Total: {} | Valid: {} | Invalid: {}",
					String.format("%-8d", translationResult.getTotalRecords()),
					String.format("%-8d", translationResult.getValidRecords()),
					String.format("%-8d", translationResult.getInvalidRecords()));
			logger.info("║                                                               ║");
			logger.info("║  GEOLOCATION:                                                 ║");
			logger.info("║    Total: {} | Valid: {} | Invalid: {}", 
			    String.format("%-8d", geolocationResult.getTotalRecords()),
			    String.format("%-8d", geolocationResult.getValidRecords()),
			    String.format("%-8d", geolocationResult.getInvalidRecords()));
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
			logger.info("║         Status: SUCCESS                                       ║");
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