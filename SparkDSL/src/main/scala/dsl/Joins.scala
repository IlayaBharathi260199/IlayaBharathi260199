package dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Joins {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("WithColumn")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val cust = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Project-Scala/files/customer.csv")
    val prod = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Project-Scala/files/product.csv")
    val source = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Project-Scala/files/Source.csv")
    val targ = spark.read.option("header", "true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Project-Scala/files/Target.csv")
      .withColumnRenamed("name", "names")
    cust.show(false)
    prod.show(false)


    println
    println("=====Inner======")
    cust.join(prod, Seq("id"), "inner").show(false)

    println
    println("=====Left======")
    cust.join(prod, Seq("id"), "left").show(false)

    println
    println("=====Right======")
    cust.join(prod, Seq("id"), "Right").show(false)

    println
    println("=====Full======")
    cust.join(prod, Seq("id"), "Full").orderBy("id").show(false)

    println
    println("=====Prob======")
    source.show(false)
    targ.show(false)

    val op = source.join(targ, Seq("id"), "Full").orderBy("id")
    op.withColumn("Comment", expr(
        """case
                                        when name=names then 'Match'
                                        when name is null then 'New in source'
                                        when names is null then 'New in target'
                                        else 'Mismatch'
                                        end """))
      .filter(col("Comment") =!= "Match")
      .drop("names", "name").show(false)

    //***left-anti join will drop common id's from left table***
    cust.join(prod, Seq("id"), "left_anti").show()


  }
}
