import org.apache.spark.sql.SparkSession

object firstDemo {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("MySparkSession")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    // Your Spark code goes here
    val df = spark.read.csv("Path")
    df.show(false)

    // Stop the SparkSession when done
    //spark.stop()
  }
}
