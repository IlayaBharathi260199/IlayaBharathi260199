package SQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark_SQL {


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("Spark_SQL")
      .master("local[*]") // Use "local[*]" to run locally using all available cores
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.option("inferschema","true").option("header","true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/df.csv")
    val df1 = spark.read.option("inferschema","true").option("header","true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/df1.csv")
    val cust = spark.read.option("inferschema","true").option("header","true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/cust.csv")
    val prod = spark.read.option("inferschema","true").option("header","true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/prod.csv")
    val emp = spark.read.option("inferschema","true").option("header","true").csv("/home/ubuntu/IdeaProjects/ilaya/IlayaBharathi260199/Scala/files/employees.csv")

//    df.show()
    df.printSchema()
//    df1.show()
//    cust.show()
//    prod.show()
// Converting DF to Table
    df.createOrReplaceTempView("df")
    df1.createOrReplaceTempView("df1")
    cust.createOrReplaceTempView("cust")
    prod.createOrReplaceTempView("prod")
    emp.createOrReplaceTempView("emp")


    spark.sql("select * from df").show()

    spark.sql("select amount,product from df").show()

    spark.sql("select amount,category from df where category='Exercise' and amount > 100 ").show()

    spark.sql("select * from df where product='GymnasticsPro' and spendby='cash'").show()

    spark.sql("select product from df where product in ('Rings','Field')").show()

    spark.sql("select product from df where product not in ('GymnasticsPro','Weightlifting')").show()

    spark.sql("select * from df where spendby like '%ca%'").show()

    spark.sql("select * from df where spendby not like '%ca%'").show()

    spark.sql("select * from df where product is null").show()

    spark.sql("select * from df where product is not null").show()

    spark.sql("select sum(amount) from df").show()

    spark.sql("select category,sum(amount) as total from df group by category").show()

    //Min
    spark.sql("select category,upper(category) from df").show() //similarly  lower

    //Max
    spark.sql("select max(amount) from df").show()

    spark.sql("select category,min(amount) from df group by category").show()

    //Count
    spark.sql("select count(1) from df").show() // count(1) for performance

    //Case statement
    spark.sql("select *,case when spendby='cash' then 1 else 0 end as status from df").show()

    //Concat
    spark.sql("select First_Name,Last_Name,concat(First_Name,' ',Last_Name) as Full_Name from emp").show()

    // Concat columns with delimiter(we can add n-number of columns)
    spark.sql("select First_Name,Last_Name,concat_ws('~',First_Name,Last_Name) as Full_Name from emp").show()

    spark.sql("select distinct spendby from df").show()

  }
}
