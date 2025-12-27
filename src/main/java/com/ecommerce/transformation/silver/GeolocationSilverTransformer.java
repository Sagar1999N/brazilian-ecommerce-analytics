// src/main/java/com/ecommerce/transformation/silver/GeolocationSilverTransformer.java
package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class GeolocationSilverTransformer extends BaseSilverTransformer {

    public GeolocationSilverTransformer(SparkSession spark) {
        super(spark, "geolocations");
    }

    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> data) {
        // Validate lat/lon ranges for Brazil
        Dataset<Row> validRecords = data.filter(
            col("geolocation_zip_code_prefix").isNotNull()
                .and(col("geolocation_lat").isNotNull())
                .and(col("geolocation_lng").isNotNull())
                .and(col("geolocation_city").isNotNull())
                .and(col("geolocation_state").isNotNull())
                .and(col("geolocation_lat").between(-35.0, 5.0)) // Brazil lat range
                .and(col("geolocation_lng").between(-75.0, -35.0)) // Brazil lng range
                .and(length(col("geolocation_state")).equalTo(2))
        );

        Dataset<Row> invalidRecords = data.except(validRecords);
        return new DataQualityResult(validRecords, invalidRecords);
    }

    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> data) {
        // Natural key: (zip_code_prefix, lat, lng) — same zip can have multiple coords
        return data.dropDuplicates("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng");
    }

    @Override
    protected Dataset<Row> enrichData(Dataset<Row> data) {
        // No enrichment needed — pure dimension table
        return data;
    }
}