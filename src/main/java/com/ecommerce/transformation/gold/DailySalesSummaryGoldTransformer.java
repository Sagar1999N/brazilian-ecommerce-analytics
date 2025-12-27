package com.ecommerce.transformation.gold;

import com.ecommerce.config.AppConfig;
import com.ecommerce.transformation.silver.BaseSilverTransformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class DailySalesSummaryGoldTransformer {
    private static final Logger logger = LoggerFactory.getLogger(DailySalesSummaryGoldTransformer.class);
    private final SparkSession spark;
    private final String silverPath;
    private final String goldPath;

    public DailySalesSummaryGoldTransformer(SparkSession spark) {
        this.spark = spark;
        this.silverPath = AppConfig.getString("app.data.silver.path", "data/silver/");
        this.goldPath = AppConfig.getString("app.data.gold.path", "data/gold/");
    }

    public BaseSilverTransformer.TransformationResult transform() {
        logger.info("=== Starting Daily Sales Summary Gold Transformation ===");
        BaseSilverTransformer.TransformationResult result = new BaseSilverTransformer.TransformationResult();

        try {
            // Read Silver tables
            Dataset<Row> orders = spark.read().parquet(silverPath + "orders");
            Dataset<Row> orderItems = spark.read().parquet(silverPath + "order_items");
            Dataset<Row> products = spark.read().parquet(silverPath + "products");
            Dataset<Row> categoryTranslation = spark.read().parquet(silverPath + "category_translation");

            // Join and aggregate
            Dataset<Row> salesSummary = orders
                .join(orderItems, "order_id")
                .join(products, "product_id")
                .join(categoryTranslation, 
                    products.col("product_category_name").equalTo(categoryTranslation.col("product_category_name")), 
                    "left")
                .groupBy(
                    date_format(col("order_purchase_timestamp"), "yyyy-MM-dd").alias("sale_date"),
                    coalesce(categoryTranslation.col("product_category_name_english"), lit("unknown")).alias("category")
                )
                .agg(
                    countDistinct("order_id").alias("orders"),
                    sum("price").alias("total_revenue"),
                    avg("price").alias("avg_order_value"),
                    count("product_id").alias("items_sold")
                );

            // Write to Gold
            String outputPath = goldPath + "daily_sales_summary";
            salesSummary.write()
                .mode(SaveMode.Overwrite)
                .parquet(outputPath);

            result.setSuccess(true);
            result.setValidRecords(salesSummary.count());
            logger.info("✅ Daily Sales Summary written to: {}", outputPath);

        } catch (Exception e) {
            logger.error("❌ Daily Sales Summary transformation failed", e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        }

        return result;
    }
}