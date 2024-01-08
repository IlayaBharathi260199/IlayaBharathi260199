package Basic

object CurryingFunction {
  def add(a:Int,b:Int) = a+b
  def summ (x:Int)(y:Int) = x + y
  def main(args:Array[String]): Unit ={
    //val a =add(5)_
    val b =summ(5)_

    //







  }

}
