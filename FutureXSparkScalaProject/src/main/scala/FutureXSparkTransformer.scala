import org.apache.spark.sql.SparkSession

object FutureXSparkTransformer {
  def main(args: Array[String]): Unit = {
    // Create a Spark Session
    // For Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // .config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport()

    val spark = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    println("Created Spark Session")
    val sampleSeq = Seq((1,"spark"),(2,"Big Data"))

    val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
    df.show()
    df.write.format("csv").save("samplesq")

  }
}
