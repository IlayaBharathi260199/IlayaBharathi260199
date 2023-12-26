package dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.functions.col


object Regex_replace {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("regex_replace")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/employees.csv")
    data.printSchema()

    val esplit = data.withColumn("bemail", split(col("Email"), "@").getItem(0))
      .withColumn("aemail", expr("split(Email,'@')[1]"))

      // in "aemail" col we have data like --> slingacademy.com, need to replace ".com" with ""

      .withColumn("aemail", regexp_replace(col("aemail"), ".com", ""))
    esplit.show(5, false)
  }
}
