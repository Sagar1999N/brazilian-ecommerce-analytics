package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class CustomerSchema {

	public static StructType getSchema() {
		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("customer_id", DataTypes.StringType, false),
				DataTypes.createStructField("customer_unique_id", DataTypes.StringType, false),
				DataTypes.createStructField("customer_zip_code_prefix", DataTypes.StringType, true),
				DataTypes.createStructField("customer_city", DataTypes.StringType, true),
				DataTypes.createStructField("customer_state", DataTypes.StringType, true));

		return DataTypes.createStructType(fields);
	}

	public static String[] getColumnNames() {
		return new String[] { 
			"customer_id", 
			"customer_unique_id", 
			"customer_zip_code_prefix", 
			"customer_city",
			"customer_state" 
		};
	}
}