import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaStructuredStreaming {

  def main(args: Array[String]): Unit = {

    // Create Spark session
    val spark = SparkSession.builder
      .appName("KafkaStructuredStreaming")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Read data from Kafka as a DataFrame
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ilaya")
      .load()

    val opp ="/home/ubuntu/ilaya/wr.csv" // outputpath
    val path_to_checkpoint_directory ="/home/ubuntu/ilaya/checkpoint" // check point path must for structured streaming


    // Write consumed data to specific path as CSV
    val query = kafkaStreamDF.selectExpr("cast( value as string)").writeStream
      .outputMode("append")
      .format("csv")
      .option("checkpointLocation", path_to_checkpoint_directory)
      .start(opp)

    // Wait for the termination of the query
    query.awaitTermination()
  }
}

