// src/main/java/com/ecommerce/transformation/silver/ProductCategoryTranslationSilverTransformer.java
package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ProductCategoryTranslationSilverTransformer extends BaseSilverTransformer {

    public ProductCategoryTranslationSilverTransformer(SparkSession spark) {
        super(spark, "category_translation");
    }

    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> data) {
        Dataset<Row> validRecords = data.filter(
            col("product_category_name").isNotNull()
                .and(col("product_category_name_english").isNotNull())
        );

        Dataset<Row> invalidRecords = data.except(validRecords);
        return new DataQualityResult(validRecords, invalidRecords);
    }

    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> data) {
        return data.dropDuplicates("product_category_name");
    }

    @Override
    protected Dataset<Row> enrichData(Dataset<Row> data) {
        // No enrichment needed — it's a simple lookup table
        return data;
    }
}