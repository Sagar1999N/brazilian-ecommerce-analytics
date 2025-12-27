// src/main/java/com/ecommerce/transformation/silver/SellersSilverTransformer.java
package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SellersSilverTransformer extends BaseSilverTransformer {

    public SellersSilverTransformer(SparkSession spark) {
        super(spark, "sellers");
    }

    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> data) {
        Dataset<Row> validRecords = data.filter(
            col("seller_id").isNotNull()
                .and(col("seller_zip_code_prefix").isNotNull())
                .and(col("seller_city").isNotNull())
                .and(col("seller_state").isNotNull())
                .and(length(col("seller_state")).equalTo(2)) // Brazilian state codes are 2 letters
        );

        Dataset<Row> invalidRecords = data.except(validRecords);
        return new DataQualityResult(validRecords, invalidRecords);
    }

    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> data) {
        return data.dropDuplicates("seller_id");
    }

    @Override
    protected Dataset<Row> enrichData(Dataset<Row> data) {
        return data
            .withColumn("seller_city_upper", upper(col("seller_city")))
            .withColumn("seller_region",
                when(col("seller_state").isin("SP", "RJ", "MG", "ES"), "SUDESTE")
                .when(col("seller_state").isin("PR", "SC", "RS"), "SUL")
                .when(col("seller_state").isin("BA", "SE", "PE", "PB", "RN", "CE", "PI", "MA", "AL"), "NORDESTE")
                .when(col("seller_state").isin("MT", "MS", "GO", "DF"), "CENTRO-OESTE")
                .otherwise("NORTE")
            );
    }
}