// src/main/java/com/ecommerce/schema/SellerSchema.java
package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class SellerSchema {
    
    public static StructType getSchema() {
        List<StructField> fields = Arrays.asList(
            DataTypes.createStructField("seller_id", DataTypes.StringType, false),
            DataTypes.createStructField("seller_zip_code_prefix", DataTypes.IntegerType, false),
            DataTypes.createStructField("seller_city", DataTypes.StringType, false),
            DataTypes.createStructField("seller_state", DataTypes.StringType, false)
        );
        return DataTypes.createStructType(fields);
    }
    
    public static String[] getColumnNames() {
        return new String[] {
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        };
    }
}