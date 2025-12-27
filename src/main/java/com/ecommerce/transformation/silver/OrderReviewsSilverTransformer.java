// src/main/java/com/ecommerce/transformation/silver/OrderReviewsSilverTransformer.java
package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class OrderReviewsSilverTransformer extends BaseSilverTransformer {

    public OrderReviewsSilverTransformer(SparkSession spark) {
        super(spark, "order_reviews");
    }

    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> data) {
        Dataset<Row> validRecords = data.filter(
            col("review_id").isNotNull()
                .and(col("order_id").isNotNull())
                .and(col("review_score").isNotNull())
                .and(col("review_score").cast(DataTypes.IntegerType).between(1, 5))
                .and(col("review_creation_date").isNotNull())
        );

        Dataset<Row> invalidRecords = data.except(validRecords);
        return new DataQualityResult(validRecords, invalidRecords);
    }

    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> data) {
        return data.dropDuplicates("review_id"); // review_id is UUID
    }

    @Override
    protected Dataset<Row> enrichData(Dataset<Row> data) {
        return data
            .withColumn("review_sentiment",
                when(col("review_score").geq(4), "positive")
                .when(col("review_score").equalTo(3), "neutral")
                .otherwise("negative")
            )
            .withColumn("review_response_time_hours",
                when(col("review_answer_timestamp").isNotNull(),
                    (unix_timestamp(col("review_answer_timestamp")) 
                     .minus(unix_timestamp(col("review_creation_date")))) 
                    .divide(3600)
                ).otherwise(null)
            );
    }
}