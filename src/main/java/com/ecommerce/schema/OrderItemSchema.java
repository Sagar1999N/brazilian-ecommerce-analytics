package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class OrderItemSchema {
    
    public static StructType getSchema() {
        List<StructField> fields = Arrays.asList(
            DataTypes.createStructField("order_id", DataTypes.StringType, false),
            DataTypes.createStructField("order_item_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("product_id", DataTypes.StringType, false),
            DataTypes.createStructField("seller_id", DataTypes.StringType, false),
            DataTypes.createStructField("shipping_limit_date", DataTypes.TimestampType, true),
            DataTypes.createStructField("price", DataTypes.DoubleType, true),
            DataTypes.createStructField("freight_value", DataTypes.DoubleType, true)
        );
        
        return DataTypes.createStructType(fields);
    }
    
	public static String[] getColumnNames() {
		return new String[] { 
			"order_id", 
			"order_item_id", 
			"product_id", 
			"seller_id",
			"shipping_limit_date",
			"price",
			"freight_value"
		};
	}
}