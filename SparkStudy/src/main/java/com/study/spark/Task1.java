package com.study.spark;

import com.study.spark.SparkSessionRunner.SparkTaskSession;
import org.apache.spark.sql.*;


public class Task1 {
    public static void main(String[] args) {

        SparkSession spark = SparkTaskSession.getInstance();
        Dataset<Row> df = spark.read().option("inferSchema", "true")
                .option("delimiter","|")
                .option("nullValue", "?")
                .option("header", true)
                .csv("file:///root/test/task_2_source.csv");
        df.write().mode(SaveMode.Overwrite).saveAsTable("horacio.task1");
        spark.close();
    }
}
