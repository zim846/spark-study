package com.study.spark;

import com.study.spark.Filter.AccountFilter;
import com.study.spark.SparkSessionRunner.SparkTaskSession;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LocalTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkTaskSession.getInstance();
        Dataset<Row> df = sparkSession.read().option("inferSchema", "true")
                .option("delimiter","|")
                .option("nullValue", "?")
                .option("header", true)
                .csv("D:\\requirement\\task_3_source.csv");

        try {
            AccountFilter accountFilter = new AccountFilter();
            df = df.filter(accountFilter);
            df = df.select(df.col(""));
        }catch (Exception e){
            e.printStackTrace();
        }

        df.show();
        sparkSession.close();
    }
}
