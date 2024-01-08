package Custom_Functions

import org.apache.spark.sql.{DataFrame, SparkSession}


object CustomReadFuction {

  def parquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

  def csv(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").csv(path)
  }


}
