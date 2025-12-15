package com.ecommerce;

import org.apache.spark.sql.SparkSession;

import com.ecommerce.config.SparkSessionFactory;

public class Main {
    public static void main(String[] args) {
        System.out.println("Brazilian E-commerce Analytics Pipeline");
        System.out.println("Project structure is ready!!!!");
        SparkSession ss = SparkSessionFactory.createSession("testing");
        System.out.println(ss.sparkContext().appName());
    }
}