// src/main/java/com/ecommerce/transformation/silver/OrderItemsSilverTransformer.java
package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class OrderItemsSilverTransformer extends BaseSilverTransformer {

    public OrderItemsSilverTransformer(SparkSession spark) {
        super(spark, "order_items");
    }

    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> data) {
        // Define valid records
        Dataset<Row> validRecords = data.filter(
            col("order_id").isNotNull()
                .and(col("order_item_id").isNotNull())
                .and(col("product_id").isNotNull())
                .and(col("seller_id").isNotNull())
                .and(col("price").cast(DataTypes.DoubleType).isNotNull())
                .and(col("freight_value").cast(DataTypes.DoubleType).isNotNull())
                .and(col("price").gt(0))
                .and(col("freight_value").geq(0))
        );

        Dataset<Row> invalidRecords = data.except(validRecords);
        return new DataQualityResult(validRecords, invalidRecords);
    }

    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> data) {
        // Natural key: (order_id, order_item_id)
        return data.dropDuplicates("order_id", "order_item_id");
    }

    @Override
    protected Dataset<Row> enrichData(Dataset<Row> data) {
        return data
            .withColumn("total_item_value", col("price").plus(col("freight_value")))
            .withColumn("price_category", 
                when(col("price").lt(50), "low")
                .when(col("price").lt(150), "medium")
                .otherwise("high")
            );
    }
}