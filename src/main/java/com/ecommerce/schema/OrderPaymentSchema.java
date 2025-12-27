// src/main/java/com/ecommerce/schema/OrderPaymentSchema.java
package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class OrderPaymentSchema {
    
    public static StructType getSchema() {
        List<StructField> fields = Arrays.asList(
            DataTypes.createStructField("order_id", DataTypes.StringType, false),
            DataTypes.createStructField("payment_sequential", DataTypes.IntegerType, false),
            DataTypes.createStructField("payment_type", DataTypes.StringType, false),
            DataTypes.createStructField("payment_installments", DataTypes.IntegerType, true),
            DataTypes.createStructField("payment_value", DataTypes.DoubleType, false)
        );
        return DataTypes.createStructType(fields);
    }
    
    public static String[] getColumnNames() {
        return new String[] {
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        };
    }
}