// src/main/java/com/ecommerce/schema/GeolocationSchema.java
package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class GeolocationSchema {
    
    public static StructType getSchema() {
        List<StructField> fields = Arrays.asList(
            DataTypes.createStructField("geolocation_zip_code_prefix", DataTypes.IntegerType, false),
            DataTypes.createStructField("geolocation_lat", DataTypes.DoubleType, false),
            DataTypes.createStructField("geolocation_lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("geolocation_city", DataTypes.StringType, false),
            DataTypes.createStructField("geolocation_state", DataTypes.StringType, false)
        );
        return DataTypes.createStructType(fields);
    }
    
    public static String[] getColumnNames() {
        return new String[] {
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        };
    }
}