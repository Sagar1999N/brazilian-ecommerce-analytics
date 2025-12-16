package com.ecommerce.ingestion;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.AppConfig;
import com.ecommerce.schema.CustomerSchema;
import com.ecommerce.schema.OrderSchema;
import com.ecommerce.schema.ProductSchema;

/**
 * Reads data files with schema validation and error handling.
 */
public class DataReader {
	private static final Logger logger = LoggerFactory.getLogger(DataReader.class);

	private final SparkSession spark;
	private final JavaSparkContext jsc;
	private final String rawDataPath;

	// File names in the dataset
	private static final Map<String, String> DATA_FILES = new HashMap<>() {
		{
			put("orders", "olist_orders_dataset.csv");
			put("customers", "olist_customers_dataset.csv");
			put("products", "olist_products_dataset.csv");
			put("order_items", "olist_order_items_dataset.csv");
			put("order_payments", "olist_order_payments_dataset.csv");
			put("order_reviews", "olist_order_reviews_dataset.csv");
			put("sellers", "olist_sellers_dataset.csv");
			put("geolocation", "olist_geolocation_dataset.csv");
			put("category_translation", "product_category_name_translation.csv");
		}
	};

	public DataReader(SparkSession spark) {
		this.spark = spark;
		this.jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		this.rawDataPath = AppConfig.getString("app.data.raw.path");
	}

	/**
	 * Reads orders data with schema validation
	 */
	public Dataset<Row> readOrders() {
		return readFile("orders", OrderSchema.getSchema());
	}

	/**
	 * Reads customers data with schema validation
	 */
	public Dataset<Row> readCustomers() {
		return readFile("customers", CustomerSchema.getSchema());
	}

	/**
	 * Reads products data with schema validation
	 */
	public Dataset<Row> readProducts() {
		return readFile("products", ProductSchema.getSchema());
	}

	/**
	 * Generic method to read any CSV file with schema validation
	 */
	public Dataset<Row> readFile(String dataType, StructType schema) {
		String fileName = DATA_FILES.get(dataType);
		if (fileName == null) {
			throw new IllegalArgumentException("Unknown data type: " + dataType);
		}

		String filePath = rawDataPath + "/" + fileName; // Added slash for path separation
		logger.info("Reading {} data from: {}", dataType, filePath);

		try {
			// Check if file exists
			if (!Files.exists(Paths.get(filePath))) {
				logger.warn("File not found: {}. Creating empty DataFrame.", filePath);
				return createEmptyDataFrame(schema);
			}

			// Read with schema validation
			Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "false") // We use explicit
																									// schema
					.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("mode", "PERMISSIVE") // Added for error
																									// handling
					.option("columnNameOfCorruptRecord", "_corrupt_record") // Capture malformed rows
					.schema(schema).csv(filePath);

			// Check for corrupt records
			if (Arrays.stream(df.columns()).anyMatch("_corrupt_record"::equals)) {
				Dataset<Row> corruptRecords = df.filter(df.col("_corrupt_record").isNotNull());
				long corruptCount = corruptRecords.count();
				if (corruptCount > 0) {
					logger.warn("Found {} corrupt records in {}", corruptCount, dataType);
					if (corruptCount <= 10) { // Only show if small number
						corruptRecords.show((int) corruptCount, false);
					}

					// Filter out corrupt records
					df = df.filter(df.col("_corrupt_record").isNull()).drop("_corrupt_record");
				}
			}

			long rowCount = df.count();
			logger.info("Successfully read {} data. Row count: {}", dataType, rowCount);
			logger.debug("Schema for {}: {}", dataType, df.schema().treeString());

			return df;

		} catch (Exception e) {
			logger.error("Failed to read {} data from {}", dataType, filePath, e);
			throw new RuntimeException("Failed to read " + dataType + " data", e);
		}
	}

	/**
	 * Creates an empty DataFrame with the specified schema
	 */
	private Dataset<Row> createEmptyDataFrame(StructType schema) {
		// Method 1: Using JavaSparkContext
		List<Row> emptyList = new ArrayList<>();
		JavaRDD<Row> emptyRDD = jsc.parallelize(emptyList);

		// Create DataFrame from empty RDD with schema
		return spark.createDataFrame(emptyRDD, schema);
	}

	/**
	 * Alternative method using emptyDataFrame API (Spark 2.0+)
	 */
	private Dataset<Row> createEmptyDataFrameOptimized(StructType schema) {
		// Method 2: Using createDataFrame with empty list (preferred)
		List<Row> emptyList = new ArrayList<>();
		return spark.createDataFrame(emptyList, schema);
	}

	/**
	 * Alternative using emptyDataset (Spark 2.0+)
	 */
	private Dataset<Row> createEmptyDataFrameUsingEmptyDataset(StructType schema) {
		// Method 3: Using emptyDataset - most efficient
		return spark.emptyDataFrame();
	}

	/**
	 * Reads file with schema inference (for exploratory analysis)
	 */
	public Dataset<Row> readWithInference(String dataType) {
		String fileName = DATA_FILES.get(dataType);
		if (fileName == null) {
			throw new IllegalArgumentException("Unknown data type: " + dataType);
		}

		String filePath = rawDataPath + "/" + fileName;

		logger.info("Reading {} with schema inference: {}", dataType, filePath);

		return spark.read().option("header", "true").option("inferSchema", "true")
				.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("mode", "PERMISSIVE")
				.option("columnNameOfCorruptRecord", "_corrupt_record").csv(filePath);
	}

	/**
	 * Lists all available data files
	 */
	public void listAvailableFiles() {
		logger.info("Available data files in {}:", rawDataPath);

		DATA_FILES.forEach((type, fileName) -> {
			String filePath = rawDataPath + "/" + fileName;
			boolean exists = Files.exists(Paths.get(filePath));
			logger.info("  {}: {} - {}", type, fileName, exists ? "Available" : "Missing");
		});
	}

	/**
	 * Read specific file with custom options
	 */
	public Dataset<Row> readFileWithOptions(String dataType, Map<String, String> options) {
		String fileName = DATA_FILES.get(dataType);
		if (fileName == null) {
			throw new IllegalArgumentException("Unknown data type: " + dataType);
		}

		String filePath = rawDataPath + "/" + fileName;
		logger.info("Reading {} with custom options: {}", dataType, filePath);

		var reader = spark.read();
		options.forEach(reader::option);

		return reader.csv(filePath);
	}

	/**
	 * Check if data file exists
	 */
	public boolean dataFileExists(String dataType) {
		String fileName = DATA_FILES.get(dataType);
		if (fileName == null) {
			return false;
		}

		String filePath = rawDataPath + "/" + fileName;
		return Files.exists(Paths.get(filePath));
	}
}