package GCP

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object ProjectGCP {


  def main(args: Array[String]): Unit = {
    // Create a SparkSession

    val Conf = new SparkConf()
      .setAppName("ProjectGCP") // Spark Application Name
      .setMaster("local[*]") // deploy mode local, using all cores for parallel processing,

    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()

    val customer = spark.read.option("inferschema", "true")
      .parquet("gs://ilaya/customer")
    //customer.show(false)

    //    customer.write.mode("Overwrite")
    //      .parquet("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/Parquet/customer")


    val product = spark.read.option("inferschema", "true")
      .parquet("gs://ilaya/product")
   // product.show()

    //    product.write.mode("Overwrite")
    //      .parquet("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/Parquet/product")

    val france_sales = spark.read.option("inferschema", "true")
      .parquet("gs://ilaya/france_sales")
      .withColumn("country", lit("FR"))
   // france_sales.show()

    //    france_sales.write.mode("Overwrite")
    //      .parquet("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/Parquet/france_sales")


    val usa_sales = spark.read.option("inferschema", "true")
      .parquet("gs://ilaya/usa_sales")
      .withColumn("country", lit("US"))
  //  usa_sales.show()

    //    usa_sales.write.mode("Overwrite")
    //      .parquet("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/Parquet/usa_sales")


    println()
    println("======Union======")
    val fr_uni_us = france_sales.union(usa_sales)
    fr_uni_us.show()

    val joinWithUni = customer.join(fr_uni_us, customer("customer_key") === fr_uni_us("customer"), "left").drop("customer")
//    println()
//    println("======Join with Union======")
//    joinWithUni.show(false)


//    println()
//    println("======Final_Joined======")
    val Final = joinWithUni.join(product, joinWithUni("product") === product("product_key"), "left").drop("product")
   // Final.show(false)

        Final.write.partitionBy("country").mode("Overwrite")
          .parquet("gs://ilaya/GCP_Project/final_Customer")

   // println("Total Number of colums: " + Final.columns.length)


  }
}
