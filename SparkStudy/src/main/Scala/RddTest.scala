import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object RddTest {
  val conf = new SparkConf().setAppName("test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val list = List(1, 2, 3, 4,11,21,231,21,22,13,43)
  val list1 = List(5, 6, 7, 8,13,24,45,65,43)

  def mapTest(rdd: RDD[Int]): Unit = {
    val result = rdd.map((_, "word")).collect().toList
    println(result)
  }

  def filterTest(rdd: RDD[Int]): Unit = {
    val list = rdd.filter(_ > 2).collect().toList
    println(list)
  }

  def flatMapTest(list: List[Int], list1: List[Int]): Unit = {
    sc.parallelize(list).flatMap(f=>{
      Array(f+0.1,f+0.2)
    }).foreach(println)
  }

  def sampleTest(rdd: RDD[Int]): Unit = {
    rdd.sample(false,0.8).foreach(println)
  }

  def mapPartitionsTest(rdd: RDD[Int]): Unit = {
    rdd.mapPartitions(iter=>{
      iter.map(_->"word")
    }).foreach(println)
  }

  def mapPartitionWithIndexText(rdd: RDD[Int]): Unit = {
    rdd.mapPartitionsWithIndex((index,iter)=>{
      iter.map(num=>{
        (num,s"index:${index}")
      })
    }).foreach(println)
  }

  def unionTest(rdd: RDD[Int], rdd1: RDD[Int]): Unit = {
    rdd.union(rdd1).foreach(println)
  }

  def intersectionTest(rdd: RDD[Int], rdd1: RDD[Int]): Unit = {
    rdd.intersection(rdd1).foreach(println)
  }

  def distinctTest(rdd: RDD[Int]): Unit = {
    rdd.distinct().foreach(println)
  }

  def groupByTest(): Unit = {
    val tuples = sc.parallelize(Array( "a" -> 1,"a" ->2, "b" -> 3)).groupBy(_._1).collect().toList
    println(tuples)
  }

  def groupByKeyTest(): Unit = {
    val tuples = sc.parallelize(Array( "a" -> 1,"a" ->2, "b" -> 3)).groupByKey().collect().toList
    println(tuples)
  }

  def reduceByKey(): Unit = {
    val tuples = sc.parallelize(Array( "a" -> 1,"a" ->2, "b" -> 3)).reduceByKey((x,y)=>{
      x+y
    }).collect().toList
    println(tuples)
  }

  def aggregateByKeyTest(): Unit = {
    val pa = sc.parallelize(Array("a" -> 1,"a" ->2, "b" -> 4),2)

    //第一个函数是map阶段
    //第二个函数是reduce阶段
    val rdd: RDD[(String, Int)] = pa.aggregateByKey(2)(
    (x: Int, y: Int) => x + y,
    (x: Int, y: Int) => x * y
    )
    rdd.foreach(println)
  }

  def sortByKeyTest(): Unit = {
    val pa = sc.parallelize(Array("b" -> 1,"d" ->2,"a" ->3, "c" -> 4))
    pa.sortByKey(false).collect.foreach(println)
  }

  def joinTest(): Unit = {
    val pa = sc.parallelize(
    Array("a" -> 1,
    "b" -> 2, "b" -> 3)
    )
    val pb = sc.parallelize(
    Array("b" -> 2, "b" -> 3,
    "d" -> 4)
    )
//    val result = pa.join(pb).collect().toList
        val result = pa.leftOuterJoin(pb).collectAsMap()
    //    val result = pa.rightOuterJoin(pb)
    //    val result = pa.fullOuterJoin(pb)
    println(result)

  }

  def cogroupTest(): Unit = {
    val pa = sc.parallelize(Array("a" -> 1, "b" -> 2, "b" -> 3))
    val pb = sc.parallelize(Array("a" -> "a", "b" -> 2, "b" -> "c"))
    val result = pa.cogroup(pb).collect.toList
    println(result)
  }

  def cartesianTest(): Unit = {
    val pa = sc.parallelize(Array(1,2))
    val pb = sc.parallelize(Array(3,4))
    val result = pa.cartesian(pb).collect.toList
    println(result)
  }


  def filterByRangeTest(): Unit = {
    val rdd1 = sc.parallelize(List(("e", 5), ("c", 3), ("d", 4), ("c", 2), ("a", 1)))
    rdd1.filterByRange("c","e").foreach(println)

  }

  def combineByKey(): Unit = {
    val rdd1 = sc.parallelize(List(1,2,2,3,3,3,3,4,4,4,4,4), 2)
    val rdd2 = rdd1.map((_, 1))
    val rdd3 = rdd2.combineByKey(-_, (x:Int, y:Int) => x + y,
    (x:Int, y:Int) => x + y)
    println(rdd2.collect.toList)
    println(rdd3.collect.toList)
  }

  def foldByKeyTest(): Unit = {
    val rdd = sc.parallelize(Array(("A", 1), ("A", 2), ("B", 1), ("B", 2), ("C", 1)))
    val result: collection.Map[String, Int] = rdd.foldByKey(1)(_+_).collectAsMap()
    println(result)
  }


  def mapValuesTest(rdd: RDD[Int], rdd1: RDD[Int]): Unit = {
    rdd.map(_->"word").mapValues(_+"test").foreach(println)
//    val result = rdd.map(_->"w").flatMapValues(_+"_").collect().toList
//    println(result)
  }

  def repartitionTest(): Unit = {
    val rdd = sc.parallelize(list)
    val rdd1 = rdd.repartition(2)
    println(rdd1.getNumPartitions)
    println(rdd1.repartition(1).getNumPartitions)
  }

  def coalesceTest(): Unit = {
    val rdd = sc.parallelize(list,2)
    println(rdd.coalesce(5,true).getNumPartitions)
    println(rdd.coalesce(1).getNumPartitions)
  }

  def keysValues(): Unit = {
    val rdd = sc.parallelize(list).map(_->"w")
    val keys: RDD[Int] = rdd.keys
    val values: RDD[String] = rdd.values
    println(keys.collect().toList)
    println(values.collect().toList)

  }

  def subtractByKey(): Unit = {
    val l1 = List(1,2,3)
    val l2 = List(2,3,4)
    val rdd = sc.parallelize(l1).map(_->"w")
    val rdd2 = sc.parallelize(l2).map(_->"w")
    rdd.subtractByKey(rdd2).foreach(println)
  }

  def countByValue(): Unit = {
    val l = List(1,2,2,3,3,4,4,4)
    val result: collection.Map[Int, Long] = sc.parallelize(l).countByValue()
    println(result)
    println(sc.parallelize(l).count())
  }

  def takeOrderd(): Unit = {
    val l = List(4,5,2,1,3)
    println("takeOrdered:"+sc.parallelize(l).takeOrdered(2).toList)
    println("take:"+sc.parallelize(l).take(2).toList)
    println("top:"+sc.parallelize(l).top(2).toList)
    println("first:"+sc.parallelize(l).first())
  }

  def lookUp(): Unit = {
    val l = List((4,"a"),(5,"b"),(2,"c"),(1,"d"),(3,"e"),(2,"f"))
    println(sc.parallelize(l).lookup(2))
  }

  def saveAsHadoopFile(): Unit = {
    val l = List((4,"a"),(5,"b"),(2,"c"),(1,"d"),(3,"e"),(2,"f"))
    //支持老版本的api
    sc.parallelize(l).repartition(1).saveAsHadoopFile("/tmp/test2",classOf[IntWritable],classOf[Text],
      classOf[TextOutputFormat[IntWritable,Text]],classOf[org.apache.hadoop.io.compress.GzipCodec])

    sc.parallelize(l).repartition(1).saveAsNewAPIHadoopFile("/tmp/test1",classOf[IntWritable],
      classOf[Text],classOf[org.apache.hadoop.mapreduce.lib.output.TextOutputFormat[IntWritable
        ,Text]])

    sc.parallelize(l).repartition(1).saveAsTextFile("/tmp/test3",
      classOf[org.apache.hadoop.io.compress.GzipCodec])

    sc.parallelize(l).repartition(1).saveAsSequenceFile("/tmp/test4")

    sc.parallelize(l).repartition(1).saveAsObjectFile("/tmp/test5")
  }

  def groupWith(): Unit = {
    val l = List((4,"a"),(5,"b"),(2,"c"),(1,"d"),(3,"e"),(2,"f"))
    println(sc.parallelize(l).groupWith(sc.parallelize(l)).collectAsMap())
  }

  def aggragate(): Unit = {
    val rdd = sc.parallelize(list,2)
    rdd.aggregate(0)(
      (x,y)=>x+y,
      (x,y)=>x*y
    )
  }


  def partitionBy(): Unit = {
    val rdd = sc.parallelize(list,2).map(_->"w")
//    rdd.repartitionAndSortWithinPartitions()
  }

  def main(args: Array[String]): Unit = {
    val rdd = sc.parallelize(list,2)
    val rdd1 = sc.parallelize(list1)

//    mapTest(rdd)
//    filterTest(rdd)
    //    flatMapTest(list, list1)//扁平化
    //    sampleTest(rdd)//抽样
    //    mapPartitionsTest(rdd)
    //    mapPartitionWithIndexText(rdd)
    //    unionTest(rdd,rdd1)
    //    intersectionTest(rdd,rdd1)//交集
//        subtractByKey()
//        distinctTest(rdd)
//        groupByTest()
//        groupByKeyTest()
//        reduceByKey()
//        aggregateByKeyTest()
//        sortByKeyTest()
//        joinTest()
//        cogroupTest()
//        cartesianTest()//笛卡尔积
//        filterByRangeTest()
//        combineByKey()
//        foldByKeyTest()
//        mapValuesTest(rdd,rdd1)
    //    repartitionTest()
    //    coalesceTest()
    //    keysValues()
//    countByValue()
//    takeOrderd()
//    lookUp()
//    saveAsHadoopFile()
//    groupWith()
//    aggragate()
//    partitionBy()
  }
}

