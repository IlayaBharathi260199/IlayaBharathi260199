package Basic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object useCase5_SchemaRDD {
  def main(args: Array[String]): Unit = {

    val Conf = new SparkConf()
      .setAppName("RDD") // Spark Application Name
      .setMaster("local[*]") // deploy mode local, using all cores for parallel processing,

    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")

     val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val file = sc.textFile("/home/ubuntu/IdeaProjects/ilaya/Scala/files/airports.text")
    //file.take(5).foreach(println)

    val len=file.filter(x=>x.length()>50)
    val count=len.count()//take(5).foreach(println)
    println("count :" + count)

    // Replace "," => "~" and "Double Quotes" => "Single Quotes"
    val fil =len.map(x=>x.replace(",","~").replace("\"","'"))
    fil.take(5).foreach(println)


  }
}
