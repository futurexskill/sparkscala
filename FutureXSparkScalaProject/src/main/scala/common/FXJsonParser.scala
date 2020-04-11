package common

import com.typesafe.config.{Config,  ConfigFactory}
import org.slf4j.LoggerFactory

object FXJsonParser {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def readJsonFile(): Config = {
    ConfigFactory.load("FX_Config.json")
  }
  def fetchPGTargetTable(): String = {
    val pgTargetTable = readJsonFile().getString("body.pg_target_table")
    pgTargetTable
  }
  def returnConfigValue(key : String): String = {
    val value = readJsonFile().getString(key)
    value
  }


}
