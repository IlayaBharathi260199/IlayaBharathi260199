import org.apache.spark.sql.SparkSession

object Dfwrite {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("DFWrite") // Spark Application Name
      .master("local[*]") // deploy mode local, using all cores for parallel processing,
      .getOrCreate() // Alternatively, you can specify a specific number instead of the asterisk, like "local[2]" if you want to use only 2 cores.

    import spark.implicits._

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    // Reading df as csv file format
    val df = spark.read.csv("Path")

    // Writing as Parquet format
    df.write.parquet("DestinationPath")

  }
}
