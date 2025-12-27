// src/main/java/com/ecommerce/transformation/silver/OrderPaymentsSilverTransformer.java
package com.ecommerce.transformation.silver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class OrderPaymentsSilverTransformer extends BaseSilverTransformer {

    public OrderPaymentsSilverTransformer(SparkSession spark) {
        super(spark, "order_payments");
    }

    @Override
    protected DataQualityResult applyDataQualityRules(Dataset<Row> data) {
        // Valid payment types in dataset
        String[] validPaymentTypes = {"credit_card", "boleto", "voucher", "debit_card"};

        Dataset<Row> validRecords = data.filter(
            col("order_id").isNotNull()
                .and(col("payment_sequential").isNotNull())
                .and(col("payment_type").isNotNull())
                .and(col("payment_value").cast(DataTypes.DoubleType).isNotNull())
                .and(col("payment_value").gt(0))
                .and(array_contains(lit(validPaymentTypes), col("payment_type")))
                .and(col("payment_sequential").gt(0))
                .and(col("payment_installments").geq(0).or(col("payment_installments").isNull()))
        );

        Dataset<Row> invalidRecords = data.except(validRecords);
        return new DataQualityResult(validRecords, invalidRecords);
    }

    @Override
    protected Dataset<Row> deduplicateRecords(Dataset<Row> data) {
        // Natural key: (order_id, payment_sequential)
        return data.dropDuplicates("order_id", "payment_sequential");
    }

    @Override
    protected Dataset<Row> enrichData(Dataset<Row> data) {
        return data
            .withColumn("payment_type_clean", 
                when(col("payment_type").equalTo("credit_card"), "Credit Card")
                .when(col("payment_type").equalTo("boleto"), "Boleto")
                .when(col("payment_type").equalTo("voucher"), "Voucher")
                .when(col("payment_type").equalTo("debit_card"), "Debit Card")
                .otherwise("Other")
            )
            .withColumn("is_installment_payment", 
                when(col("payment_installments").gt(1), true)
                .otherwise(false)
            );
    }
}