package dsl

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Word_count {


  def main(args: Array[String]): Unit = {

    val Conf = new SparkConf()
      .setAppName("Word_Count") // Spark Application Name
      .setMaster("local[*]") // deploy mode local, using all cores for parallel processing,

    // Create a SparkContext
    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")


    // Create a SparkSession
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    // Reading data as RDD(Text file)
    val csv = sc.textFile("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/cust.csv")
    csv.foreach(println)

    // Flattening the data (RDD)
    val flat = csv.flatMap(x => x.split(","))
    // flat.foreach(println)

    // Converting RDD to DF
    val df = flat.toDF()
    // df.printSchema()

    // Df transformation for Word_Count
    val word_count = df.withColumnRenamed("value", "Words")
      .groupBy("Words").count().orderBy("count") //for descending .orderBy(desc("count"))

    word_count.show(false)

  }
}
