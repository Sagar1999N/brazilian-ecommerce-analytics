package com.ecommerce.jobs;

import java.io.File;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.AppConfig;
import com.ecommerce.config.SparkSessionFactory;
import com.ecommerce.ingestion.DataReader;
import com.ecommerce.ingestion.DataValidator;
import com.ecommerce.ingestion.DatasetDownloader;

/**
 * Main job for data ingestion and validation
 */
public class DataIngestionJob {
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
		logger = LoggerFactory.getLogger(DataIngestionJob.class);
	}

	public static void main(String[] args) {
		logger.info("Starting Data Ingestion Job");

		SparkSession spark = null;

		try {
			// 1. Initialize Spark
			spark = SparkSessionFactory.createSession("ingestion");

			// 2. Download dataset (if needed)
			logger.info("Step 1: Downloading dataset...");
			DatasetDownloader.downloadDataset();

			// 3. Create data reader
			DataReader dataReader = new DataReader(spark);

			// 4. List available files
			dataReader.listAvailableFiles();

			// 5. Read and validate each dataset
			logger.info("Step 2: Reading and validating data...");

			// Read orders
			Dataset<Row> orders = dataReader.readOrders();
			logger.info("Orders sample:");
			orders.show(5);

			// Read customers
			Dataset<Row> customers = dataReader.readCustomers();
			logger.info("Customers sample:");
			customers.show(5);

			// Read products
			Dataset<Row> products = dataReader.readProducts();
			logger.info("Products sample:");
			products.show(5);

			// 6. Validate data quality
			logger.info("Step 3: Validating data quality...");
			DataValidator validator = new DataValidator();

			DataValidator.ValidationResult ordersValidation = validator.validateDataQuality(orders, "orders");
			ordersValidation.logResults();

			DataValidator.ValidationResult customersValidation = validator.validateDataQuality(customers, "customers");
			customersValidation.logResults();

			DataValidator.ValidationResult productsValidation = validator.validateDataQuality(products, "products");
			productsValidation.logResults();

			// 7. Save validated data to staging area
			logger.info("Step 4: Saving to staging area...");

			String stagingPath = AppConfig.getString("app.data.bronze.path");// "data/staging/";

			orders.write().mode(SaveMode.Overwrite).parquet(stagingPath + "orders");
			logger.info("Orders saved to: {}", stagingPath + "orders");

			customers.write().mode(SaveMode.Overwrite).parquet(stagingPath + "customers");
			logger.info("Customers saved to: {}", stagingPath + "customers");

			products.write().mode(SaveMode.Overwrite).parquet(stagingPath + "products");
			logger.info("Products saved to: {}", stagingPath + "products");

			logger.info("Data ingestion completed successfully!");

		} catch (Exception e) {
			logger.error("Data ingestion job failed", e);
			System.exit(1);
		} finally {
			if (spark != null) {
				SparkSessionFactory.stopSession(spark);
			}
		}
	}
}