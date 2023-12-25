package Pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object RDMS_Spark_HDFS {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName("ReadFromPostgres")
      .config("spark.master", "local") // Set your Spark master URL
      .getOrCreate()

    // Set log level to avoid unnecessary logs
    spark.sparkContext.setLogLevel("ERROR")


    //Template
    //==========
    /* val jdbcUrl = "jdbc:postgresql://your_postgres_host:your_postgres_port/your_database"
     val connectionProperties = new java.util.Properties()
     connectionProperties.setProperty("user", "your_username")
     connectionProperties.setProperty("password", "your_password")
     */

    // Extract
    val jdbcUrl = "jdbc:postgresql://localhost:5432/dvdrental"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "postgres")
    connectionProperties.setProperty("password", "5602")

    val df = spark.read.jdbc(jdbcUrl, "customer", connectionProperties)
    df.printSchema()


    // Transformation
    val trans=df.groupBy("active").count()
    trans.show(false)

    val rowcount = df.count()
    println(s"Number of rows in the DataFrame: $rowcount")
    println(f"Number of rows in the DataFrame: " + rowcount)

    // Writing transformed data to HDFS
    trans.write.partitionBy("").parquet("HDFS PATH")


  }
}
