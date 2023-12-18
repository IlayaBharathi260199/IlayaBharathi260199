package Basic

object scala {

  def main(args: Array[String]): Unit = {

    val a = 5
    println(a)
    val b = 6
    println
    println(b)

    val c = a + b
    println
    println(c)


    // how to add 20 to each elements in "mylist" ?
    val mylist = List(1, 2, 3, 4)
    println
    println(mylist)


    // using LAMBDA(=>) we can achive this
    val modlist = mylist.map(x => x + 20)
    println
    println(modlist)

    //String concat
    val myStringlist = List("ilayabha", "bharathi", "ilaya")
    val modlist1 = myStringlist.map(x => x + " you are good")
    println(myStringlist)
    println
    println(modlist1)
    println

    //Replace with LAMBDA(=>)
    val mylistReplace = myStringlist.map(x => x.replace("ilaya", "bharathi"))
    println(mylistReplace)

    // want only data contains "ilaya"
    val mylistcont = myStringlist.filter(x => x.contains("ilaya"))
    println(mylistcont)
    mylistcont.foreach(println)


     println()
    println("=======is========")

    val newlist =mylist.map(x=> x + "is" + myStringlist)
    newlist.foreach(println)

  }
}
