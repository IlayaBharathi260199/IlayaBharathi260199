package DSL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Aggregation {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Aggregation")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val dt = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/dt.csv")
          dt.show(false)

    dt.groupBy("category")
      .agg(sum("amount").cast("Int").as("amount"),count("amount")
        .as("count")).orderBy(col("amount") desc).show(false)


  }
}
