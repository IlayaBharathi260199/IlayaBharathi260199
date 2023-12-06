package Basic
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



/*

RDD
===

INPUT
=====
 val file = sc.textFile("/home/ubuntu/IdeaProjects/ilaya/Scala/files/statecity.csv")

Expected OUTPUT should be
=========================
   Create two lists one with states and other with cities
   ======States=======
    TN
    Gujarat

   ======City=======
    Chennai
    GandhiNagar
 */
object useCase3_SparkEntry {

  def main(args: Array[String]): Unit = {

    val Conf = new SparkConf()
      .setAppName("test") // Spark Application Name
      .setMaster("local[*]") // deploy mode local, using all cores for parallel processing,

    val sc =new SparkContext(Conf)
    sc.setLogLevel("ERROR")

    val file = sc.textFile("/home/ubuntu/IdeaProjects/ilaya/Scala/files/statecity.csv")
    val flatm = file.flatMap(x => x.split("~"))
    val state = flatm.filter(x => x.contains("State"))
    val city = flatm.filter(x => x.contains("City"))

    val States = state.map(x => x.replace("State->", ""))
    val Cities = city.map(x => x.replace("City->", ""))

    println("======States=======")
    States.foreach(println)
    println
    println("======City=========")
    Cities.foreach(println)

  }

}

