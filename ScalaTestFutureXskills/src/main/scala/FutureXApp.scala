import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.example.FutureXTransformer

object FutureXApp {
  def main(args: Array[String]): Unit = {
    println("FutureX ScalaTest demo")

    val spark = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    println("Created Spark Session")

    // Define schema
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("course_name", StringType, nullable = false),
      StructField("author_name", StringType, nullable = true),
      StructField("no_of_reviews", IntegerType, nullable = true)
    ))

    // Define data
    val data = Seq(
      Row(1, "Java", "FutureX", 45),
      Row(2, "Java", "FutureXSkill", 56),
      Row(3, "Big Data", "Future", 100),
      Row(4, "Linux", "Future", 100),
      Row(5, "Microservices", "Future", 100),
      Row(6, "CMS", "", 100),
      Row(7, "Python", "FutureX", null),
      Row(8, "CMS", "Future", 56),
      Row(9, "Dot Net", "FutureXSkill", 34),
      Row(10, "Ansible", "FutureX", 123),
      Row(11, "Jenkins", "Future", 32),
      Row(12, "Chef", "FutureX", 121),
      Row(13, "Go Lang", "", 105)
    )

    // Create DataFrame
    val courseDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show DataFrame
    courseDF.show()

    println("Replacing Null Values")
    val transformedDF = FutureXTransformer.replaceNullValues(courseDF)
    transformedDF.show()


  }

}
