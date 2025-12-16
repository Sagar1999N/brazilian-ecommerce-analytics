package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * Defines the schema for orders dataset.
 * Ensures consistent schema across the application.
 */
public class OrderSchema {
    
    public static StructType getSchema() {
        List<StructField> fields = Arrays.asList(
            DataTypes.createStructField("order_id", DataTypes.StringType, false),
            DataTypes.createStructField("customer_id", DataTypes.StringType, false),
            DataTypes.createStructField("order_status", DataTypes.StringType, true),
            DataTypes.createStructField("order_purchase_timestamp", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_approved_at", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_delivered_carrier_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_delivered_customer_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("order_estimated_delivery_date", DataTypes.TimestampType, true)
        );
        
        return DataTypes.createStructType(fields);
    }
    
    public static String[] getColumnNames() {
        return new String[] {
            "order_id",
            "customer_id", 
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        };
    }
}