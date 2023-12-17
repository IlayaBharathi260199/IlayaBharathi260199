package Pipeline

import org.apache.spark.sql.SparkSession

object RDMS_Spark_HDFS {




  val spark = SparkSession.builder
    .appName("ReadFromPostgres")
    .config("spark.master", "local") // Set your Spark master URL
    .getOrCreate()

  val jdbcUrl = "jdbc:postgresql://your_postgres_host:your_postgres_port/your_database"
  val connectionProperties = new java.util.Properties()
  connectionProperties.setProperty("user", "your_username")
  connectionProperties.setProperty("password", "your_password")

  val df = spark.read.jdbc(jdbcUrl, "your_table", connectionProperties)

  df.show()

}
