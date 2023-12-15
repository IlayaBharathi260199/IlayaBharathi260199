package complex


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexdataProcessing {


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Aggregation")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val dt = spark.read.option("multiline", "true").json("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/actors.json")
    println("===Raw Schema===")
    dt.printSchema()

    val flat1=dt.withColumn("Actors",expr("explode(Actors)"))
    flat1.printSchema()

   val flat2= flat1.select(
      "Actors.Birthdate",
      "Actors.Born At",
      "Actors.age",
      "Actors.hasChildren",
      "Actors.hasGreyHair",
      "Actors.name",
      "Actors.children",
      "Actors.photo",
      "Actors.weight",
      "Actors.wife",
      "country",
      "version"
    )

    flat2.printSchema()

    val flat3 =flat2.withColumn("children",explode(col("children")))
    println("===flattened Schema===")
    flat3.printSchema()

   // flat3.write.csv("gs://ilaya/actors_flatten_Data")



  }
}
