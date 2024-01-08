package Basic

import org.apache.spark.sql._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

//HIGHER ORDER FUNCTION
//=====================
//Higher order fuction is a function that takes function as argument.
object HigherOrderFunction {
  def add(a: Int, b: Int, fun: (Int, Int) => Int) = fun(a, b)

  //        1st ,  2nd  ,  3rd(func)
  def summ(x: Int, y: Int) = x + y

  def parent(df: DataFrame, f: DataFrame => DataFrame): DataFrame =
    f(df.withColumn("Position",lit("Senior")))

  def child(df: DataFrame): DataFrame = {
    df.filter(col("Age") > 30).withColumn("Age",col("Age") + 2)
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark_SQL")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/employees.csv")

    println("========First=======")

    println(add(5, 90, summ))
    //              1,2 , 3

    println("======DF=========")

    val lk = parent(df, child)
    lk.show(false)

    println("======List=======")
    val l = List(1, 2, 3, 4)

    def mul(y: Int) = y * y

    val fin=l.map(x => mul(x))
      fin.foreach(println)


  }

}
