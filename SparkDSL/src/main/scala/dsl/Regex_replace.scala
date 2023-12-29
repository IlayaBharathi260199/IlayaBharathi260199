package dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


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

    // to remove "Numerical" values from data pattern = "\\d"(common)
    val num = data.withColumn("Email", regexp_replace(col("Email"), "\\d", ""))
    println()
    println("=====Numbers Removed=====")
    num.show(false)

    // To remove Special characters from data pattern = "[^a-zA-Z ]"(common)
    // Specify the special characters you want to remove
    // val specialCharacters = "[,\\!@]" (cal val name in pattern without "")
    val speChar = data.withColumn("Email", regexp_replace(col("Email"), "[^a-zA-Z ]", ""))
    println()
    println("=====Special characters Removed=====")
    speChar.show(false)
  }
}
