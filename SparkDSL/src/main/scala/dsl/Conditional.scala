package dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//*****when_otherwise***** its a kind of if else

object Conditional {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("when_otherwise")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read.option("header", "true").option("inferschema","true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/employees.csv")
    data.printSchema()
    //data.show(2,false)


    val condition=data.withColumn("Adult",when(col("Age") >= 30,"Senior").otherwise("Junior"))
    condition.show(false)

    val minimum=data.agg(min("Age")).show(false)

  }
}
