
import org.apache.spark.sql.SparkSession


object SparkS3_Integration {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("S3_Integration")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val csv = spark.read.format("csv").option("header", "true")
      .option("fs.s3a.access.key", "AKIAVBE2M5VFZ7NJKX2I")
      .option("fs.s3a.secret.key", "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")
      .load("s3a://ilaya2/dt.csv")

    val csv1 = spark.read.format("csv").option("header", "true")
      .option("fs.s3a.access.key", "AKIAVBE2M5VFZ7NJKX2I")
      .option("fs.s3a.secret.key", "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")
      .load("s3a://ilaya2/df1.csv")


    csv.show(false)
    csv1.show(false)

    csv1.select(" product").show(false)


  }
}
