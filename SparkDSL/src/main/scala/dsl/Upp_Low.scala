package dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//*****Upper_Lower*****

object Upp_Low {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Upper_Lower")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read.option("header", "true").option("inferschema", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/employees.csv")
    data.printSchema()
    //data.show(2,false)

    data.withColumn("Last_Name", upper(col("Last_Name")))
      .withColumn("First_Name", lower(col("First_Name")))
      .select("First_Name", "Last_Name").show(false)

    // distinct
    data.select("Department").distinct().show(false)

    val cou=data.distinct().count()
    println("Distinct Count: "+ cou)

    data.orderBy(col("Department").desc).show(false)


  }
}
