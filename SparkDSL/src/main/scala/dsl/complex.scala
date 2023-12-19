package dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object complex {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Complex")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val jsondf = spark.read.option("multiline", "true").json("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/jk.json")
    jsondf.show(false)

    val Addjsondf = spark.read.option("multiline", "true").json("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/address2.json")
    println()
    println("===Addresas Raw Schema===")
    Addjsondf.printSchema()

    val flatjson=Addjsondf.selectExpr(
      "address.billing_address.address as billing_address",
      "address.billing_address.city as billing_city",
      "address.billing_address.postal_code as billing_postal_code",
      "address.billing_address.state as billing_state",
      "address.shipping_address.address shipping_address",
      "address.shipping_address.city shipping_city",
      "address.shipping_address.postal_code shipping_postal_code",
      "address.shipping_address.state as shipping_state",
      "age",
      "date_of_birth",
      "email_address",
      "first_name",
      "height_cm",
      "is_alive",
      "last_name"
    )
    Addjsondf.printSchema()

    //Raw Schema
    println()
    println("===Raw Schema===")
    jsondf.printSchema()

    println()
    println("===Address FRaw Schema===")
    flatjson.printSchema()

    val flat=jsondf.select("id",
        "institute",
        "location.*",  // selectiing all clos from location struct
        "worklocation")
    flat.show(false)
    //flatten schema
    println()
    println("===flatten schema===")
    flat.printSchema()

    val struc = flat.select(
      col("id"),col("institute"),
      struct(col("permanentLocation"),col("temporaryLocation"),col("worklocation")).as("location")
    )
    println()
    println("===newly generated schema===")
    struc.printSchema()



  }
}
