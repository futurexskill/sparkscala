package common

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

class SparkTransformerSpec extends FutureXBase {
  val spark = SparkSession.builder.appName("HelloSpark")
    .config("spark.master", "local").enableHiveSupport()
    .getOrCreate()

  def fixture = new {
    val df : DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema","true")
      .csv("mock_course_data.csv")
  }

  behavior of "Spark Transformer"

  it should "replace null value with unknown" in {

    val f = fixture
    val transformedDF = SparkTraformer.replaceNullValues(f.df)
    transformedDF.show()
    val authors = transformedDF
      .filter(transformedDF("course_id")
        .equalTo("2"))
      .select("author_name")
      .collectAsList()

    val author = authors.get(0)(0)
    println("transformed author "+author)
    assert("Unknown" == author)
  }

  it should "throw NullPointerException" in {
    try {
      val df: DataFrame = null
      val transformedDF = SparkTraformer.replaceNullValues(df)
    } catch {
      case e: NullPointerException =>
        println("NullPointerException caught")
    }
  }

  it should "also throw NullPointerException" in {

    assertThrows[NullPointerException] {
      val df: DataFrame = null

      val transformedDF = SparkTraformer.replaceNullValues(df)
    }
  }
  it should "also replace null value with unknown" in {
    val f = fixture
    val transformedDF = SparkTraformer.replaceNullValues(f.df)
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
  it should "2  also replace null value with unknown" in {
    val f = fixture
    val transformedDF = SparkTraformer.replaceNullValues(f.df)
    transformedDF.show()
    val authors = transformedDF
      .filter(transformedDF("course_id")
        .equalTo("2"))
      .select("author_name")
      .collectAsList()

     "Unknown" should equal (authors.get(0)(0))
  }

  }
