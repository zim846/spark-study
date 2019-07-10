package com.study.spark.SparkSessionRunner;

import com.study.spark.UDF.RemoveCommaUDF;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class UDFRegister {
    public static void registerAll(SparkSession sparkSession){
        sparkSession.udf().register("removeCommaUDF",new RemoveCommaUDF(), DataTypes.StringType);
    }
}
