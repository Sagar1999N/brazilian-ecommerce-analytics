// src/main/java/com/ecommerce/jobs/GoldTransformationJob.java
package com.ecommerce.jobs;

import java.io.File;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.SparkSessionFactory;
import com.ecommerce.transformation.gold.CustomerTimezoneEnrichedGoldTransformer;
import com.ecommerce.transformation.gold.DailySalesSummaryGoldTransformer;

public class GoldTransformationJob {
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
		logger = LoggerFactory.getLogger(GoldTransformationJob.class);
	}

	public static void main(String[] args) {

		logger.info("╔═══════════════════════════════════════════════════════════════╗");
		logger.info("║      GOLD TRANSFORMATION JOB - STARTED                        ║");
		logger.info("║      Transforming: Daily Sales Summary & Customer Timezone    ║");
		logger.info("╚═══════════════════════════════════════════════════════════════╝");

		long startTime = System.currentTimeMillis();

		logger.info("🚀 Starting Gold Transformation Job");

		SparkSession spark = SparkSessionFactory.createSession("gold-transformation");
		boolean success = true;

		try {
			// Transform Daily Sales Summary
			logger.info("📊 Transforming Daily Sales Summary");
			DailySalesSummaryGoldTransformer salesTransformer = new DailySalesSummaryGoldTransformer(spark);
			var salesResult = salesTransformer.transform();
			if (!salesResult.isSuccess()) {
				logger.error("❌ Daily Sales Summary failed");
				success = false;
			}

			// Transform Customer Timezone
			logger.info("🌍 Transforming Customer Timezone");
			CustomerTimezoneEnrichedGoldTransformer tzTransformer = new CustomerTimezoneEnrichedGoldTransformer(spark);
			var tzResult = tzTransformer.transform();
			if (!tzResult.isSuccess()) {
				logger.error("❌ Customer Timezone failed");
				success = false;
			}

			if (!success) {
				System.exit(1);
			}
			logger.info("✅ Gold layer complete! Output:");
			
			// Calculate and log job duration
			long endTime = System.currentTimeMillis();
			long durationSeconds = (endTime - startTime) / 1000;
			logger.info("");
			logger.info("╔═══════════════════════════════════════════════════════════════╗");
			logger.info("║         GOLD TRANSFORMATION JOB - COMPLETED                   ║");
			logger.info("║         Duration: {} seconds", String.format("%-30s", durationSeconds));
			logger.info("║         Status: SUCCESS                                       ║");
			logger.info("╚═══════════════════════════════════════════════════════════════╝");
			logger.info("");
			logger.info("✓ Gold layer is ready!");
			logger.info("  → data/gold/daily_sales_summary/");
			logger.info("  → data/gold/customer_timezone_enriched/");
			logger.info("");

		} finally {
			spark.stop();
		}
	}
}