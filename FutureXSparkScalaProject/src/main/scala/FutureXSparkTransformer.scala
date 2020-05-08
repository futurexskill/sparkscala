import java.util.Properties

import common.{FXJsonParser, InputConfig, PostgresCommon, SparkCommon, SparkTraformer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object FutureXSparkTransformer {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      logger.info("main method started")
      logger.warn("This is a warning")
      val arg_length = args.length

      if (arg_length == 0 ) {
        logger.warn("No Argument passed")
        System.exit(1)
      }

      val inputConfig : InputConfig = InputConfig(env = args(0),targetDB = args(1))

      val spark = SparkCommon.createSparkSession(inputConfig).get
      // Create Course Hive Table
      //SparkCommon.createFutureXCourseHiveTable(spark)

      val CourseDF = SparkCommon.readFutureXCourseHiveTable(spark).get
      CourseDF.show()

      // Replace Null Value

      val transformedDF1 = SparkTraformer.replaceNullValues(CourseDF)
      transformedDF1.show()

      if (inputConfig.targetDB == "pg") {
        logger.info("Writing to PG Table")
        val pgCourseTable = FXJsonParser.fetchPGTargetTable()
        logger.warn("******** pgCourseTable **** is "+pgCourseTable)

        PostgresCommon.writeDFToPostgresTable(transformedDF1,pgCourseTable)
      } else if (inputConfig.targetDB == "hive") {
        logger.info("Writing to Hive Table")

        // Write to a Hive Table
        SparkCommon.writeToHiveTable(spark,transformedDF1,"customer_transformed")
        logger.info("Finished writing to Hive Table..in main method")

      }


      //transformedDF1.write.format("csv").save("transformed-df")


      //val pgTable = "futureschema.futurex_course_catalog"
      //val pgCourseDataframe : DataFrame = PostgresCommon
      //  .fetchDataFrameFromPgTable(spark,pgTable).get
      //logger.info("Fetched PG DataFrame...logger")
      //pgCourseDataframe.show()
    } catch {
      case e:Exception =>
        logger.error("An error has occured in the main method "+ e.printStackTrace())
    }
  }
}
