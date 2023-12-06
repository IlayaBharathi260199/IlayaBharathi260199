package Basic
/*
INPUT
=====
 val mylist = List("Bigdata-Spark-Hive","Spark-Hadoop-Hive","Sqoop-Hive-Spark","Sqoop-BD-Hive")

Expected OUTPUT should be
=========================
Tech-->Bigdata Trainer ->Ilaya
Tech-->Spark Trainer ->Ilaya
Tech-->Hive Trainer ->Ilaya
Tech-->Hadoop Trainer ->Ilaya
Tech-->Sqoop Trainer ->Ilaya
Tech-->BD Trainer ->Ilaya
 */
object useCase1 {

  def main(args: Array[String]): Unit = {

    // input
    val mylist = List("Bigdata-Spark-Hive","Spark-Hadoop-Hive","Sqoop-Hive-Spark","Sqoop-BD-Hive")
    val flatlist= mylist.flatMap(x=>x.split("-")).distinct
    val concatlist=flatlist.map( x=> "Tech-->" + x + " Trainer ->Ilaya")
    concatlist.foreach(println)

  }

}

