import org.apache.spark.sql.SparkSession
import Custom_Functions.CustomReadFuction._

object ReadWithFunction {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Spark_SQL")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val Parqdata = parquet(spark, "/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/Parquet/customer")
    Parqdata.show(false)

    val csvdata = csv(spark, "/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/cust.csv")
    csvdata.show(false)


  }

}
