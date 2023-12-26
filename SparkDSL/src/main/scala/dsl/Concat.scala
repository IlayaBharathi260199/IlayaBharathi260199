package dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.functions.col


object Concat {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("concat")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")


    val data = spark.read.option("header","true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/employees.csv")
         data.printSchema()

    // 2 ways of concat wirh "expr" and "concat method"
    val conc =data.withColumn("Full_Name",expr("concat(`First Name`,' ',`Last Name`)"))
    val conc1 =data.withColumn("Full_Name",concat(col("First Name"),lit(" "),col("Last Name")))

    conc.show(1,false)
    conc1.show(1,false)


  }
}
