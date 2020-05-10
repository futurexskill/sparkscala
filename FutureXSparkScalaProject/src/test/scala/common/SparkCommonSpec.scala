package common

import org.apache.spark.sql.SparkSession

class SparkCommonSpec extends FutureXBase {
  behavior of "Spark Common"

  it should "create a session" in {
    val inputConfig : InputConfig = InputConfig(env = "dev",targetDB = "pg")
    val spark = SparkCommon.createSparkSession(inputConfig).get
  }
}
