package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class DailySalesSummarySchema {
    
    public static StructType getSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("sale_date", DataTypes.DateType, false),
            DataTypes.createStructField("category", DataTypes.StringType, false),
            DataTypes.createStructField("orders", DataTypes.LongType, false),
            DataTypes.createStructField("total_revenue", DataTypes.DoubleType, false),
            DataTypes.createStructField("avg_order_value", DataTypes.DoubleType, false),
            DataTypes.createStructField("items_sold", DataTypes.LongType, false)
        ));
    }
}