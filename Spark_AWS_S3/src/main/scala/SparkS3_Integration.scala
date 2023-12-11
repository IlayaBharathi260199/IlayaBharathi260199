
import org.apache.spark.sql.SparkSession


object SparkS3_Integration {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("S3_Integration")
      .config("fs.s3a.access.key", "Your Key")
      .config("fs.s3a.secret.key", "Your Key")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")


    //1.Reading from AWS S3
    val csv = spark.read.format("csv").option("header", "true").load("s3a://ilaya2/dt.csv")
    val csv1 = spark.read.format("csv").option("header", "true").load("s3a://ilaya2/df1.csv")
    csv1.show()
    csv.show()

    //2.===========Transformations===========


    //3.Writing to AWS S3 as Parq
     csv.write.mode("overwrite").parquet("s3a://ilaya2/df1.parq")

  }
}
