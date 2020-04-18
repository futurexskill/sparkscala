object ScalaBasics {
  def main(args: Array[String]): Unit = {
    println("Learning Scala Basics")
    //demoVarVal()
    //demoVarVal()
    //variableDeclaration()
    val ssMain = demoLoops("If for")
    println(ssMain)
  }

  def demoLoops(str : String): String = {

    if (10==10) {
      println("inside if")
    } else if (10==7){
      println("inside else if")
    } else {
      println("inside else")
    }

    if ((10==10) && (7==5)) {
      println("inside if")
    } else if (7==7){
      println("inside else if")
    } else {
      println("inside else")
    }

    for( i <- 1 to 10)
    {
      println("Inside for loop - i " + i)
    }


    for( i <- 1 until 5){
      println("demo until  "+ i)
    }

    for(i <- List(10,20,30,40)) {
      println("Demoing for loop using a List . i " + i)
    }

    var i = 1
    while( i < 5 ){
      println ("demo while loop  " + i)
      i= i+1
    }

    val ss = "Loop demo done "+ str
    ss
  }

  def demoVarVal(): Unit = {
    var a = 3
    val b = 5
    a= 8
    println("a "+ a)
    println("b "+b)
  }

  def variableDeclaration(): Unit = {
    val a = 3
    val b : Int = 5
    println("a "+ a)
    println("b "+b)
    val c : Double = 4.5
    val d : Double = 5
    println("c " + c)
    println("d " + d)

    var myArray : Array[Int] = Array(10,20,30,40)
    println("myArray index 0 " + myArray(0))
    println("myArray index 1 " +myArray(1))
    println("myArray index 2 " +myArray(2))

    var myArray2 : Array[String] = Array("a","b","c","d")
    println("myArray2 index 0 " + myArray2(0))
    println("myArray2 index 1 " +myArray2(1))
    println("myArray2 index 2 " +myArray2(2))

    val myList1 = List(10,20,30,40)

    val myList2 = List(10,20,"Big Data",40)

    println("myList 1  " + myList1(0))
    println("myList 2  " + myList2(2))
    println("myList 2  " + myList2(3))

    println("myList 2 size is " + myList2.size)

    val my_tuple = (1,2,3,4)
    println("my_tuple 1  " + my_tuple._1)

    val mapValue = Map(("key1",10),("key2",20))

    println("mapValue  Key 2 is " + mapValue("key2"))

  }

}
