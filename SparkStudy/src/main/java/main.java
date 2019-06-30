import Utils.CsvUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.ArrayList;

public class main {
    public static void main(String[] args) {
        ArrayList<String> list = CsvUtils.importCsv(new File("D:\\requirement\\task_2_source.csv"));
        for (String data : list) {
            System.out.println("data = " + data);
        }

        SparkSession spark = SparkSession.builder().master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = spark.read().option("inferSchema", "true")
                //		   可设置分隔符，默认，
                .option("delimiter","|")
                //          设置空值
                .option("nullValue", "?")
                //          表示有表头，若没有则为false
                .option("header", true)
                //          文件路径
                .csv("D:\\requirement\\task_2_source.csv");


        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people WHERE ACCOUNT =111101");
        sqlDF.show();
        spark.close();
    }
}
