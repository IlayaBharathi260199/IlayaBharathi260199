package DSL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Joins {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("WithColumn")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val csv = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/dt.csv")
    csv.show(false)

    csv.withColumn("tdate",expr("split(tdate,'-')[2]"))
      .withColumnRenamed("tdate","year")
      .withColumn("Status",expr("case when spendby = 'cash' then 1 else 0 end")).show(false)



  }
}
