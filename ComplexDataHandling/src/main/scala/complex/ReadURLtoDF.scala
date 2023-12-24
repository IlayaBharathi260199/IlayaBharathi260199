package complex

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source // Need, to read data from URL

object ReadURLtoDF {


  def main(args: Array[String]): Unit = {
    // Create a SparkSession

    val Conf = new SparkConf()
      .setAppName("SchemaRDD") // Spark Application Name
      .setMaster("local[*]") // deploy mode local, using all cores for parallel processing,

    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    //Reading data from URL as String ,using common template(line:26,27)
    val htmlSource = Source.fromURL("https://randomuser.me/api/0.8/?results=20")
    val fileAsString = htmlSource.mkString
    // println(fileAsString)

    // Converting URL data(string) to RDD
    val toRDD = sc.parallelize(List(fileAsString))
    //  toRDD.foreach(println)


    // Reading Above RDD as DataFrame
    val df = spark.read.json(toRDD)
    df.printSchema()
    df.show()

    //flattening pending....?????

  }
}
