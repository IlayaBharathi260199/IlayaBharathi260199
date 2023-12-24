package Pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object HiveWarehouse_Spark_AWS_S3 {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Hive_Integration")
    //  .config("fs.s3a.access.key", "AKIAVBE2M5VFSGJHXMEP")
    //  .config("fs.s3a.secret.key", "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")
      .enableHiveSupport()
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()


    spark.conf.set("spark.sql.warehouse.dir", "hdfs://localhost:50000/user/hive/warehouse")

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    // Set up MySQL as the Hive metastore

    // Read data from the Hive table
    val hiveTableName = "hive.db.hivetable"
    val hiveDF = spark.sql(s"SELECT * FROM $hiveTableName")

    // Show the DataFrame
    hiveDF.show()


  }


}
