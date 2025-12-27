// src/main/java/com/ecommerce/transformation/gold/CustomerTimezoneEnrichedGoldTransformer.java
package com.ecommerce.transformation.gold;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Optional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecommerce.config.AppConfig;
import com.ecommerce.transformation.silver.BaseSilverTransformer;
import com.ecommerce.util.TimeZoneConstants;

import net.iakovlev.timeshape.TimeZoneEngine;

public class CustomerTimezoneEnrichedGoldTransformer {
	private static final Logger logger = LoggerFactory.getLogger(CustomerTimezoneEnrichedGoldTransformer.class);
	private static final TimeZoneEngine ENGINE = TimeZoneEngine.initialize();
	private final SparkSession spark;
	private final String silverPath;
	private final String goldPath;

	public CustomerTimezoneEnrichedGoldTransformer(SparkSession spark) {
		this.spark = spark;
		this.silverPath = AppConfig.getString("app.data.silver.path", "data/silver/");
		this.goldPath = AppConfig.getString("app.data.gold.path", "data/gold/");
	}

	public BaseSilverTransformer.TransformationResult transform() {
		logger.info("=== Starting Customer Timezone Enriched Gold Transformation ===");
		BaseSilverTransformer.TransformationResult result = new BaseSilverTransformer.TransformationResult();

		try {
			// Register UDF
			UDF3<String, Double, Double, String> timezoneUDF = (country, lat, lon) -> {
				if (lat == null || lon == null)
					return "unknown";

				// Use hardcoded mapping for single-timezone countries
				if (TimeZoneConstants.SINGLE_TIMEZONE_COUNTRIES.containsKey(country)) {
					String zoneIdStr = TimeZoneConstants.SINGLE_TIMEZONE_COUNTRIES.get(country);
					return getAbbreviation(zoneIdStr);
				}

				// Use TimeShape for multi-timezone countries
				Optional<ZoneId> zoneIdOpt = ENGINE.query(lat, lon);
				if (zoneIdOpt.isPresent()) {
					return getAbbreviation(zoneIdOpt.get().getId());
				}
				return "unknown";
			};

			spark.udf().register("derive_timezone", timezoneUDF, DataTypes.StringType);

			// Read Silver tables
			Dataset<Row> customers = spark.read().parquet(silverPath + "customers");
			Dataset<Row> geolocation = spark.read().parquet(silverPath + "geolocations").select(
					"geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_state",
					"geolocation_city");

			// Join and enrich
			Dataset<Row> enriched = customers
					.join(geolocation,
							customers.col("customer_zip_code_prefix")
									.equalTo(geolocation.col("geolocation_zip_code_prefix")),
							"left")
					.withColumn("timezone", callUDF("derive_timezone", lit("BR"), // Brazil hardcoded for demo
							col("geolocation_lat"), col("geolocation_lng")));

			// Write to Gold
			String outputPath = goldPath + "customer_timezone_enriched";
			enriched.write().mode(SaveMode.Overwrite).parquet(outputPath);

			result.setSuccess(true);
			result.setValidRecords(enriched.count());
			logger.info("✅ Customer Timezone Enriched written to: {}", outputPath);

		} catch (Exception e) {
			logger.error("❌ Customer Timezone Enriched transformation failed", e);
			result.setSuccess(false);
			result.setErrorMessage(e.getMessage());
		}

		return result;
	}

	private static String getAbbreviation(String zoneIdStr) {
		try {
			ZoneId zoneId = ZoneId.of(zoneIdStr);
			ZonedDateTime now = ZonedDateTime.now(zoneId);
			return now.getZone().getDisplayName(TextStyle.SHORT, Locale.ENGLISH);
		} catch (Exception e) {
			return "error";
		}
	}
}