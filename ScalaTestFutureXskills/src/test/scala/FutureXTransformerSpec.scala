package common
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.FutureXTransformer
import exceptions.DataValidationException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


class FutureXTransformerSpec extends FutureXBase {

  val spark = SparkSession.builder.appName("ScalaTest")
    .config("spark.master", "local").enableHiveSupport()
    .getOrCreate()

  def fixture = new {
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("mock_course_data.csv")
    df.show()

  }


  behavior of "Spark Transformer"

  it should "replace null value in author name with unknown" in {


    val f = fixture

    val transformedDF = FutureXTransformer.replaceNullValues(f.df)
    transformedDF.show()
    val authors = transformedDF
      .filter(transformedDF("course_id")
        .equalTo("2"))
      .select("author_name")
      .collectAsList()

    val author = authors.get(0)(0)
    println("transformed author " + author)
    assert("Unknown" == author)


  }

  it should "throw NullPointerException" in {
    try {
      val df: DataFrame = null
      val transformedDF = FutureXTransformer.replaceNullValues(df)
    } catch {
      case e: NullPointerException =>
        println("NullPointerException caught")
    }
  }

  it should "Also throw NullPointerException" in {

    assertThrows[DataValidationException] {
      val df: DataFrame = null
      val transformedDF = FutureXTransformer.replaceNullValues(df)

    }


  }
  it should "also replace null value in author name with unknown" in {


    val f = fixture

    val transformedDF = FutureXTransformer.replaceNullValues(f.df)
    transformedDF.show()
    val authors = transformedDF
      .filter(transformedDF("course_id")
        .equalTo("2"))
      .select("author_name")
      .collectAsList()

    assertResult("Unknown") {
      authors.get(0)(0)
    }


  }

  it should "2 also replace null value in author name with unknown" in {


    val f = fixture

    val transformedDF = FutureXTransformer.replaceNullValues(f.df)
    transformedDF.show()
    val authors = transformedDF
      .filter(transformedDF("course_id")
        .equalTo("2"))
      .select("author_name")
      .collectAsList()

    "Unknown" should equal(authors.get(0)(0))


  }

  it should "throw invalid data exception" in {

    val schema = StructType(List(
      StructField("course_id", IntegerType, nullable = false),
      StructField("course_name", StringType, nullable = true),
      StructField("author_name", StringType, nullable = true),
      StructField("no_of_reviews", IntegerType, nullable = true)
    ))

    val data = Seq(
      Row(1, "Jenkins", "Future", 2),
      Row(2, "CMS", null, 100),
      Row(3, null, "FutureX", 50)
    )

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    assertThrows[DataValidationException] {
      print("Catching DataValidationException")
      val transformedDF = FutureXTransformer.replaceNullValues2(testDF)

    }


  }

  it should "intercept error message " in {

    val schema = StructType(List(
      StructField("course_id", IntegerType, nullable = false),
      StructField("course_name", StringType, nullable = true),
      StructField("author_name", StringType, nullable = true),
      StructField("no_of_reviews", IntegerType, nullable = true)
    ))

    val data = Seq(
      Row(1, "Jenkins", "Future", 2),
      Row(2, "CMS", null, 100),
      Row(3, null, "FutureX", 50)
    )

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val exception = intercept[DataValidationException] {
      println("Catching DataValidationException")
      val transformedDF = FutureXTransformer.replaceNullValues2(testDF)
    }

    assert(exception.getMessage.contains("Invalid Data"))


  }
}
/*
  it should "sample test 1" in {
    fail()
  }

  it should "sample test 2" in {
    fail()
  }
  }
*/


