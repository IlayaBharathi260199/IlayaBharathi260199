package Basic

/*
INPUT
=====
 val mylist = List("State->TN~City->Chennai","State->Gujarat~City->GandhiNagar")

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
object useCase2 {

  def main(args: Array[String]): Unit = {

    // input
    val mylist = List("State->TN~City->Chennai", "State->Gujarat~City->GandhiNagar")
    val flatm = mylist.flatMap(x => x.split("~"))
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

