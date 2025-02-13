package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import exceptions.DataValidationException

object FutureXTransformer {

  def replaceNullValues(dataFrame: DataFrame): DataFrame = {
    // Replace null or empty strings with default values
    val transformedDF = dataFrame
      .na.fill("Unknown", Seq("author_name")) // Replace nulls in author_name with "Unknown"
      .withColumn("author_name",
        when(col("author_name") === "", "Unknown").otherwise(col("author_name"))) // Replace empty strings in author_name with "Unknown"
      .na.fill(0, Seq("no_of_reviews")) // Replace nulls in no_of_reviews with 0

    transformedDF
  }

  def replaceNullValues2 (dataFrame: DataFrame): DataFrame = {
    // Check for invalid data (e.g., missing course_name)
    if (dataFrame.filter(col("course_name").isNull || col("course_name") === "").count() > 0) {
      throw new DataValidationException("Invalid Data: 'course_name' cannot be null or empty")
    }

    // Replace empty author_name with "Unknown" and null no_of_reviews with 0
    dataFrame.withColumn("author_name", when(col("author_name") === "" || col("author_name").isNull, "Unknown").otherwise(col("author_name")))
      .withColumn("no_of_reviews", when(col("no_of_reviews").isNull, 0).otherwise(col("no_of_reviews")))
  }

}