// src/main/java/com/ecommerce/schema/OrderReviewSchema.java
package com.ecommerce.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class OrderReviewSchema {
    
    public static StructType getSchema() {
        List<StructField> fields = Arrays.asList(
            DataTypes.createStructField("review_id", DataTypes.StringType, false),
            DataTypes.createStructField("order_id", DataTypes.StringType, false),
            DataTypes.createStructField("review_score", DataTypes.IntegerType, false),
            DataTypes.createStructField("review_comment_title", DataTypes.StringType, true),
            DataTypes.createStructField("review_comment_message", DataTypes.StringType, true),
            DataTypes.createStructField("review_creation_date", DataTypes.TimestampType, false),
            DataTypes.createStructField("review_answer_timestamp", DataTypes.TimestampType, true)
        );
        return DataTypes.createStructType(fields);
    }
    
    public static String[] getColumnNames() {
        return new String[] {
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp"
        };
    }
}