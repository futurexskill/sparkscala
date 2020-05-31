import org.apache.spark.sql.SparkSession

object SparkScalaDemo {
  def main(args: Array[String]): Unit = {
    println("Hello Spark Scala")

    // Create a Spark Session
    val spark = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.master", "local")
      .getOrCreate()
    println("Created Spark Session")
    val sampleSeq = Seq((1,"spark"),(2,"Big Data"))

    val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
    df.show()


  }

}
