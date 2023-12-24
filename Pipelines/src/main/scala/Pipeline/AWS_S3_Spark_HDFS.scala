package Pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, current_timestamp}


object AWS_S3_Spark_HDFS {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("AWS_S3_Spark_HDFS")
      .config("fs.s3a.access.key", "AKIAVBE2M5VFSGJHXMEP")
      .config("fs.s3a.secret.key", "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")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")


    //1.Reading from AWS S3
    val csv = spark.read.format("csv").option("header", "true").load("s3a://ilaya2/dt.csv")
    //val csv1 = spark.read.format("csv").option("header", "true").load("s3a://ilaya2/df1.csv")
    csv.show()


    //2.===========Transformations===========
     val mod=csv.withColumn("timestamp",current_timestamp())
    mod.show(false)


    //3.Writing to HDFS as Parquet
   mod.write.mode("overwrite").parquet("hdfs://localhost:50000/user/ubuntu/Parq1")


  }


}
