package com.study.spark.Filter;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class AccountFilter implements FilterFunction<Row> {
    @Override
    public boolean call(Row s) throws Exception {
        Row row = (Row) s;
        int index = row.fieldIndex("KM_DM");
        String account =String.valueOf(row.get(index));
        return !account.startsWith("8")&&!account.startsWith("9");
//        return false;
    }
}
