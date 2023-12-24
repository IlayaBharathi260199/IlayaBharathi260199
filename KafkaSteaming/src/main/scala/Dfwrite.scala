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
    val df = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/dt.csv")

    // Reading df1 as csv file format from HDFS
    val df1 = spark.read.option("header", "true").csv("hdfs://localhost:50000/user/ubuntu/airports.text")
        df1.show(false)
         df1.printSchema()

    // Writing as Parquet format to HDFS
    df1.write.parquet("hdfs://localhost:50000/user/ubuntu/Parq")

  }
}
