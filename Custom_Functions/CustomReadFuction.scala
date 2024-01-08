package Custom_Functions

import org.apache.spark.sql.{DataFrame, SparkSession}


object CustomReadFuction {

  def Parquet(spark:SparkSession,path:String):DataFrame ={
        spark.read.parquet(path)
  }

}
