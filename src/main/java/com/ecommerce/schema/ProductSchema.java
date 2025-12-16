package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class ProductSchema {
    
    public static StructType getSchema() {
        List<StructField> fields = Arrays.asList(
            DataTypes.createStructField("product_id", DataTypes.StringType, false),
            DataTypes.createStructField("product_category_name", DataTypes.StringType, true),
            DataTypes.createStructField("product_name_lenght", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_description_lenght", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_photos_qty", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_weight_g", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_length_cm", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_height_cm", DataTypes.IntegerType, true),
            DataTypes.createStructField("product_width_cm", DataTypes.IntegerType, true)
        );
        
        return DataTypes.createStructType(fields);
    }
    
	public static String[] getColumnNames() {
		return new String[] { 
			"product_id", 
			"product_category_name", 
			"product_name_lenght", 
			"product_description_lenght",
			"product_photos_qty",
			"product_weight_g",
			"product_length_cm",
			"product_height_cm",
			"product_width_cm"
		};
	}
}