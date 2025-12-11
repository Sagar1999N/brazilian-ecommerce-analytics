package com.ecommerce;

import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        System.out.println("Brazilian E-commerce Analytics Pipeline");
        System.out.println("Project structure is ready!");
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("e-commerce").getOrCreate();
        System.out.println(sparkSession.sparkContext().appName());
    }
}