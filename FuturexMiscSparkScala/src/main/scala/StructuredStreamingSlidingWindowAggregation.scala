import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, from_unixtime, unix_timestamp, window}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object StructuredStreamingSlidingWindowAggregation {

  def main(args: Array[String]): Unit = {

    println("Structured Streaming Demo")

    val conf = new SparkConf().setAppName("Spark Structured Streaming").setMaster("local[*]")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    println("Spark Session created")

    val schema = StructType(Array(StructField("phone",StringType),StructField("sale_volume",IntegerType)))

    // Create a "inputDir" under the
    val streamDF = spark.readStream.option("header","true")
      .schema(schema).csv("C:\\inputDir")

    import spark.implicits._

    val streamDFTS = streamDF.withColumn("datetime" ,from_unixtime(unix_timestamp(current_timestamp(), "yyyyMMdd'T'HHmmss:SSSSSS")) )

    val streamDFTSWindow = streamDFTS.groupBy(window($"datetime","5 minutes", " 2 minutes"))
      .sum("sale_volume")

    val query = streamDFTSWindow.writeStream.format("console").outputMode(OutputMode.Complete()).start()


    query.awaitTermination()

  }

}
