// src/main/java/com/ecommerce/schema/CategoryTranslationSchema.java
package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class CategoryTranslationSchema {
    
    public static StructType getSchema() {
        List<StructField> fields = Arrays.asList(
            DataTypes.createStructField("product_category_name", DataTypes.StringType, false),
            DataTypes.createStructField("product_category_name_english", DataTypes.StringType, false)
        );
        return DataTypes.createStructType(fields);
    }
    
    public static String[] getColumnNames() {
        return new String[] {
            "product_category_name",
            "product_category_name_english"
        };
    }
}