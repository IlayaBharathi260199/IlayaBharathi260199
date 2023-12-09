package DSL

import org.apache.spark.sql.SparkSession


object SparkDSL {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("MySparkSession")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

         val parq =spark.read.parquet("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/Parquet/gym.parq")
          parq.write.orc("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/ORC/gym.orc")

        val orc =spark.read.orc("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/ORC/gym.orc")
           orc.show(false)
  }
}
