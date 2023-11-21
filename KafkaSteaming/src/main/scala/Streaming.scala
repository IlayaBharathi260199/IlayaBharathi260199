import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkConf, SparkContext}


object Streaming {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Streaming")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts","true") // Allowing multiple contexts

    // Create a SparkContext using the SparkConf
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("Error")

    // Create a SparkSession
     val spark = SparkSession
       .builder()
      .getOrCreate()

    import spark.implicits._
    // Set log level to avoid unnecessary logs
      spark.sparkContext.setLogLevel("ERROR")

    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "firstgroup",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("ilaya")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val streamValue=stream.map(record => record.value())

    streamValue.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
