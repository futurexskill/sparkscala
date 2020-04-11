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
      val spark = SparkCommon.createSparkSession().get
      // Create Course Hive Table
      //SparkCommon.createFutureXCourseHiveTable(spark)

      val CourseDF = SparkCommon.readFutureXCourseHiveTable(spark).get
      CourseDF.show()

      // Replace Null Value

      val transformedDF1 = SparkTraformer.replaceNullValues(CourseDF)
      transformedDF1.show()
      //val pgCourseTable = "futureschema.futurex_course"
      val pgCourseTable = FXJsonParser.fetchPGTargetTable()
      logger.warn("******** pgCourseTable **** is "+pgCourseTable)

      PostgresCommon.writeDFToPostgresTable(transformedDF1,pgCourseTable)


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
