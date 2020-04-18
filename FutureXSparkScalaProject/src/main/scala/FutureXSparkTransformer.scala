import java.util.Properties

import common.{FXJsonParser, PostgresCommon, SparkCommon, SparkTraformer}
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

      val env_name = args(0)

      logger.info("The Environment is "+env_name )

      val spark = SparkCommon.createSparkSession(env_name).get
      // Create Course Hive Table
      //SparkCommon.createFutureXCourseHiveTable(spark)

      val CourseDF = SparkCommon.readFutureXCourseHiveTable(spark).get
      CourseDF.show()

      // Replace Null Value

      val transformedDF1 = SparkTraformer.replaceNullValues(CourseDF)
      transformedDF1.show()
      //val pgCourseTable = "futureschema.futurex_course"
      //val pgCourseTable = FXJsonParser.fetchPGTargetTable()
      //logger.warn("******** pgCourseTable **** is "+pgCourseTable)

      //PostgresCommon.writeDFToPostgresTable(transformedDF1,pgCourseTable)

      logger.info("Writing to CSV File ")

      transformedDF1.write.format("csv").save("transformed-df")

      logger.info("Writing to Hive Table")

      // Write to a Hive Table
      SparkCommon.writeToHiveTable(spark,transformedDF1,"customer_transformed")
      logger.info("Finished writing to Hive Table..in main method")

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
