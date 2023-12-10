package DSL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col


object SelectExpr {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("MySparkSession")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val csv = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/dt.csv")
    csv.show(false)

    //selectExpr() we do some operations while selecting the column but in select() we can only select the column
    val split = csv.selectExpr("id",
      "CASE WHEN amount > 100 then amount END as gtt",
      "amount/100 as div",
      "concat(split(tdate,'-')[1],'-',split(tdate,'-')[2]) as tdate",
      "upper(product)",
      "lower(spendby)",
      "initcap(spendby)")

    val split2 = csv.withColumn("tdate", expr("split(tdate,'-')[2]"))
    split2.show(false)
  }
}
