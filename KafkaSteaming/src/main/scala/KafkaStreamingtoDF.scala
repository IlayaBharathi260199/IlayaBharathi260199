import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

// Set the log level to DEBUG

object KafkaStreamingtoDF {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Streaming")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts","true") // Allowing multiple contexts

    // Create a SparkContext using the SparkConf
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("Error")

    Logger.getRootLogger.setLevel(Level.DEBUG)

    // Create a SparkSession
     val spark = SparkSession
       .builder()
      .getOrCreate()
    import spark.implicits._
    // Set log level to avoid unnecessary logs
      spark.sparkContext.setLogLevel("ERROR")

    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    val kafkaParams = Map[String, Object](
      "bootstrap.-" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "df2group",
      "auto.offset.reset" -> "earliest"
     // "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("ilaya") // topic name

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val streamValue=stream.map(x => x.value())

    streamValue.print()

     streamValue.foreachRDD(x =>
     if (! x.isEmpty() )
       {
         val df=x.toDF("id").withColumn("time",current_timestamp())
         df.show()

       }

     )

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
