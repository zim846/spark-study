package com.study.spark;

import com.study.spark.Utils.CsvUtils;

import java.io.File;
import java.util.ArrayList;

public class ReadCsv {
    public static void main(String[] args) {
        ArrayList<String> list = CsvUtils.importCsv(new File("D:\\output\\kj_kmjk_.csv2\\part-00000-9e191249-fa56-44fb-89e8-218fa62cb0aa-c000.csv"));
        for (String data : list) {
            System.out.println("data = " + data);
        }
    }
}
