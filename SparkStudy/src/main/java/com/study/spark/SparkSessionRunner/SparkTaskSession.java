package com.study.spark.SparkSessionRunner;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.SparkSession;

public class SparkTaskSession {
    private volatile static SparkSession sparkSession;
    public static SparkSession getInstance(){
        if (sparkSession == null) {
            synchronized (SparkSession.class){
                if (sparkSession == null) {
                    sparkSession = SparkSession.builder()
                            .master("local")
//                            .enableHiveSupport()
                            .appName("Tasks")
                            .getOrCreate();
                    UDFRegister.registerAll(sparkSession);
                }
            }
        }
        return sparkSession;
    }
}
