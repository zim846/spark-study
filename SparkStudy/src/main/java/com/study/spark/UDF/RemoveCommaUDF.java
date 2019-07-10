package com.study.spark.UDF;

import org.apache.spark.sql.api.java.UDF1;

public class RemoveCommaUDF implements UDF1<String,String> {

    @Override
    public String call(String s) throws Exception {
        if (s == null)
            return null;
        s = s.replace(",","");
        return s;
    }
}
