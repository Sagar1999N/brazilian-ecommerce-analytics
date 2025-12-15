package com.ecommerce.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;

import java.util.Map;

/**
 * Factory for creating SparkSession with proper configuration. Ensures
 * consistent Spark configuration across all jobs.
 */
public class SparkSessionFactory {
	private static final Logger logger = LoggerFactory.getLogger(SparkSessionFactory.class);

	public static SparkSession createSession(String appNameSuffix) {
		String appName = AppConfig.getString("app.spark.app-name") + "-" + appNameSuffix;
		String master = AppConfig.getString("app.spark.master");

		logger.info("Creating SparkSession for app: {}, master: {}", appName, master);

		// Base configuration
		SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master);

		// Add all configured Spark properties
		Map<String, String> sparkConfigs = AppConfig.getSparkConfigs();
		sparkConfigs.forEach(sparkConf::set);

		// Build SparkSession
		SparkSession.Builder builder = SparkSession.builder().config(sparkConf);

		// Enable Hive support if configured
		if (AppConfig.getBoolean("app.spark.enable-hive-support", false)) {
			builder.enableHiveSupport();
		}

		SparkSession spark = builder.getOrCreate();

		// Set log level based on configuration
		String logLevel = AppConfig.getString("app.logging.level", "WARN");
		spark.sparkContext().setLogLevel(logLevel);

		// Get Spark UI URL (handling Scala Option properly in Java)
		Option<String> uiUrlOption = spark.sparkContext().uiWebUrl();
		String uiUrl = uiUrlOption.isDefined() ? uiUrlOption.get() : "Not available";

		logger.info("SparkSession created successfully. Spark UI: {}", uiUrl);

		return spark;
	}

	public static void stopSession(SparkSession spark) {
		if (spark != null) {
			try {
				spark.stop();
				logger.info("SparkSession stopped successfully");
			} catch (Exception e) {
				logger.error("Error stopping SparkSession", e);
			}
		}
	}
}