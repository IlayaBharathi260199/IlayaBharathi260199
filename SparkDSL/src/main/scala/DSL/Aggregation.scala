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
      .agg(sum("amount").cast("Int").as("amount"), count("amount")
        .as("count")).orderBy(col("amount") desc).show(false)

    val mail = spark.read.format("csv").option("header", "true").load("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/mail.csv")
    mail.show(false)

    val spi = mail.withColumn("split", expr("split(mail,'@')[0]"))
      .withColumn("first", expr("substring(split,1,1)"))
      .withColumn("last", expr("substring(split,-1,1)"))
      .withColumn("nfirst", expr("substring(phonenumber,0,2)"))
      .withColumn("nlast", expr("substring(phonenumber,length(phonenumber)-1,2)"))
      .withColumn("mailsize", expr("length(split) - 2"))
      .withColumn("mailf", expr("concat(first,repeat('*',mailsize),last,'@gmail.com')"))
      .withColumn("numf", expr("concat(nfirst,repeat('*',6),nlast)"))
      .drop("split", "first", "last", "size", "mailsize", "nlast", "nfirst", "phonenumber", "mail")
    spi.show(false)




  }
}
