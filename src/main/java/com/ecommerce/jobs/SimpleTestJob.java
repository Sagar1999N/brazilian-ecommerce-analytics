package com.ecommerce.jobs;

import java.io.File;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.SparkSessionFactory;
import com.ecommerce.ingestion.DatasetDownloader;

/**
 * SIMPLEST POSSIBLE job to verify our setup works
 */
public class SimpleTestJob {
	// DON'T create logger here yet
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
		}
		logger = LoggerFactory.getLogger(SimpleTestJob.class);
		System.out.println("=== LOG4J CONFIGURED ===");
	}

	public static void main(String[] args) {
		logger.info("=== Starting SIMPLE Test Job ===");
		// 1. Create Spark Session
		SparkSession spark = SparkSessionFactory.createSession("simple-test");

		try {
			// 2. Create sample data
			logger.info("Creating sample data...");
			DatasetDownloader.downloadDataset();

			// 3. Read ONE file (orders only for now)
			String filePath = "data/raw/olist_orders_dataset.csv";
			Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "true").csv(filePath);

			// 4. Show basic info
			logger.info("DataFrame loaded successfully!");
			logger.info("Number of rows: {}", df.count());
			logger.info("Schema:");
			df.printSchema();

			logger.info("First 5 rows:");
			df.show(5);

			// 5. Save to staging (simple version)
			df.write().mode(SaveMode.Overwrite).parquet(".//data//staging//simple_orderss//");
			logger.info("Saved to: data/staging/simple_orders");

			logger.info("=== SIMPLE Test Job COMPLETED SUCCESSFULLY ===");

		} catch (Exception e) {
			logger.error("Test job failed", e);
			System.exit(1);
		} finally {
			SparkSessionFactory.stopSession(spark);
		}
	}
}