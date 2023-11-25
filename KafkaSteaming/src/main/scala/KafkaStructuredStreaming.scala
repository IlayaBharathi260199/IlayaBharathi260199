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
      .format("kafka")     //kafka
      .option("kafka.bootstrap.servers", "localhost:9092")   //kafka server
      .option("subscribe", "ilaya")// kafka topic
      .option("auto.offset.reset", "latest") //consumption model we have "latest" and "earliest"
       .load()

    /* Default Schema
    kafkaStreamDF.printSchema()

    Column	            Type
    key	                binary
    value	              binary
    topic	              string
    partition	          int
    offset	            long
    timestamp	          timestamp
    timestampType	      int
    headers (optional)	array
     */
    val opp ="Path" // output path eg:"/home/ubuntu/connect/testing.csv"

    //if you wanna change WRITE path, Make sure to change CHECKPOINT path as well.

    val path_to_checkpoint_directory ="Check Point Path" // check point path is must for structured streaming eg: /home/ubuntu/ilaya/checkpoint1/


    // Write consumed data to specific path as CSV
    val query = kafkaStreamDF.selectExpr("cast( value as string)").writeStream
      .outputMode("append")
      .format("csv") // format("console") will show op in terminal , we modify file format like (parq and etcc)
      .option("checkpointLocation", path_to_checkpoint_directory)
      .start(opp) // have to call op path here

    // Wait for the termination of the query
    query.awaitTermination()
  }
}

