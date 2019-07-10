import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.reflect.internal.util.TableDef.Column

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val df = spark.read.option("inferSchema", "true")
      .option("delimiter", "|")
      .option("header", true)
      .csv("D:\\requirement\\task_2_source.csv")

//    df.createOrReplaceTempView("data")
//    val sqlDf = spark.sql("select * from data")
//    sqlDf.show(10)
    df.agg(Map("ACCOUNT"->"max")).show()
    spark.close()
  }

}