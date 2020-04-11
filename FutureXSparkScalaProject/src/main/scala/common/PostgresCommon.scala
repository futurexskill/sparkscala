package common

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object PostgresCommon {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def getPostgresCommonProps() : Properties = {
    logger.info("getPostgresCommonProps() started")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","admin")
    pgConnectionProperties
  }

  def getPostgresServerDatabase() : String = {
    logger.info("getPostgresServerDatabase() started")

    val pgURL = "jdbc:postgresql://localhost:5432/futurex"
    pgURL
  }
  def fetchDataFrameFromPgTable(spark : SparkSession, pgTable : String ) : Option[DataFrame] = {
    try {
      logger.info("fetchDataFrameFromPgTable() started")
      val pgProp = getPostgresCommonProps
      val pgURLdetails = getPostgresServerDatabase()
      logger.info("Creating Dataframe from Postgres")
      val pgCourseDataframe = spark.read.jdbc(pgURLdetails,pgTable,pgProp)
      Some(pgCourseDataframe)

    } catch {
      case e: Exception =>
        logger.error("An error has occured in fetchDataFrameFromPgTable "+ e.printStackTrace())
        System.exit(1)
        None
    }
  }
  def writeDFToPostgresTable(dataFrame: DataFrame, pgTable : String ): Unit = {
    try {
      logger.warn("writeDFToPostgresTable method started")

      dataFrame.write
        .mode(SaveMode.Append)
        .format("jdbc")
        .option("url",getPostgresServerDatabase() )
        .option("dbtable", pgTable)
        .option("user", "postgres")
        .option("password", "admin")
        .save()
      logger.warn("writeDFToPostgresTable method ended")

    } catch {
      case e: Exception =>
        logger.error("An error occured in writeDFToPostgresTable "+ e.printStackTrace())
    }
  }


}
