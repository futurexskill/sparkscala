package common

import exceptions.InvalidEnvironmentException
import org.apache.spark.sql.SparkSession

class SparkCommonSpec extends FutureXBase {
  behavior of "Spark Common"

  it should "create a session" in {
    val inputConfig : InputConfig = InputConfig(env = "dev",targetDB = "pg")
    val spark = SparkCommon.createSparkSession(inputConfig).get
  }

  it should "throw invalid environment exception" in {

    val inputConfig : InputConfig = InputConfig(env = "abc",targetDB = "pg")

    assertThrows[InvalidEnvironmentException] {
      val spark = SparkCommon.createSparkSession(inputConfig).get
    }

  }
  it should "check error message for invalid environment" in {

    val inputConfig : InputConfig = InputConfig(env = "abc",targetDB = "pg")

    val exception = intercept[InvalidEnvironmentException] {
      val spark = SparkCommon.createSparkSession(inputConfig).get
    }

    assert(exception.isInstanceOf[InvalidEnvironmentException])
    assert(exception.getMessage.contains("pass a valid environment"))

    }


}
