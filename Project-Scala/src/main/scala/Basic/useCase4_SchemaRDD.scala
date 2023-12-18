package Basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object useCase4_SchemaRDD {
  case class schema(
                     col0: String,
                     col1: String,
                     col2: String,
                     col3: String,
                     col4: String,
                     col5: String,
                   )

  def main(args: Array[String]): Unit = {

    val Conf = new SparkConf()
      .setAppName("SchemaRDD") // Spark Application Name
      .setMaster("local[*]") // deploy mode local, using all cores for parallel processing,

    val sc = new SparkContext(Conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val file = sc.textFile("/home/ubuntu/IdeaProjects/ilaya/Scala/files/dr.csv")
    file.foreach(println)

    val gym = file.filter(x => x.contains("Gymnastics"))
    gym.foreach(println)
    println

    val colsplit = gym.map(x => x.split(","))

    val addcol = colsplit.map(x => schema(x(0), x(1), x(2), x(3), x(4), x(5)))

    val filtereddata = addcol.filter(x => x.col4.contains("Gymnastics"))
    println("=============final============")
    filtereddata.foreach(println)

    val df = filtereddata.toDF()
    df.show()

    df.write.mode("append").parquet("/home/ubuntu/IdeaProjects/ilaya/Scala/files/Parquet/gym.parq")
  }
}
