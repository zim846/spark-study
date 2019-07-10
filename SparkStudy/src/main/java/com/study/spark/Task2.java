package com.study.spark;
import com.study.spark.Filter.AccountFilter;
import com.study.spark.SparkSessionRunner.SparkTaskSession;
import org.apache.spark.sql.*;

public class Task2 {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkTaskSession.getInstance();

        Dataset<Row> df1 = sparkSession.read().option("inferSchema", "true")
                .option("delimiter","|")
                .option("nullValue", "?")
                .option("header", true)
//                .csv("file:///root/test/kj_ztjk_.csv");
                .csv("D:\\requirement\\kj_ztjk_.csv");

        Dataset<Row> df2 = sparkSession.read().option("inferSchema", "true")
                .option("delimiter","|")
                .option("nullValue", "?")
                .option("header", true)
//                .csv("file:///root/test/task_2_source.csv");
                .csv("D:\\requirement\\task_2_source.csv");

        df1 = df1.select("ND_DM");
        df2 = df2.select(functions.col("ACCOUNT"),functions.callUDF("removeCommaUDF",functions.col("DESCR"))
                ,functions.col("TYPE"),functions.col("TYPE"));
        Dataset<Row> finalDF = df1.crossJoin(df2).withColumn("FJKM_DM", functions.lit(""))
                .toDF("ND_DM","KM_DM","KMMC","KMLX","KMFX","FJKM_DM")
                .filter(new AccountFilter());
//        df.write().mode(SaveMode.Overwrite).saveAsTable("horacio.task2");
        df2.show();;
        finalDF.show();
        sparkSession.close();
    }
}
