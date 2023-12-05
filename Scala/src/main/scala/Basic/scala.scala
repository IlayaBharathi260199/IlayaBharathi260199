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
    val mylist = List(1,2,3,4)
    println
    println(mylist)

    // using LAMBDA(=>) we can achive this
    val modlist = mylist.map(x => x + 20)
    println
    println(modlist)



  }
}
