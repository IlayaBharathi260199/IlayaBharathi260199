package dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.functions.col


object Filter {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("MySparkSession")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val parq = spark.read.parquet("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/Parquet/gym.parq")
    //  parq.write.orc("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/ORC/gym.orc")

    val orc = spark.read.orc("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/ORC/gym.orc")
    //  orc.show(false)

    val csv = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/dt.csv")
    csv.show(false)

    // Equals
    csv.filter(col("category") === "Exercise").show(false)
    csv.filter(!(col("category") === "Exercise")).show(false) // we can also use "=!="

    println
    println("===&&===")
    // AND(&&) and OR(||)
    csv.filter(col("category") === "Exercise" && col("spendby") === "cash").show(false)
    csv.filter(col("category") === "Exercise" || col("spendby") === "cash").show(false)

    // isin and notin
    csv.filter(col("category") isin("Exercise", "Gymnastics")).show(false)
    csv.filter(!(col("category") isin("Exercise", "Gymnastics"))).show(false)

    println
    println("===nULL===")
    csv.filter(col("product").isNull ).show(false)
    csv.filter(col("product").isNotNull).show(false)

    //like and notlike
    csv.filter(col("product") like "%Gymnastics%").show(false)
    csv.filter(!(col("product") like "%Gymnastics%")).show(false)

    //contains
    csv.filter(col("product") contains "Gymnastics").show(false)

  }
}
