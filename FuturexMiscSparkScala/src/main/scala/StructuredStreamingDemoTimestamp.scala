import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, unix_timestamp}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object StructuredStreamingDemoTimestamp {

  def main(args: Array[String]): Unit = {

    println("Structured Streaming Demo")

    val conf = new SparkConf().setAppName("Spark Structured Streaming").setMaster("local[*]")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    println("Spark Session created")

    val schema = StructType(Array(StructField("empId",StringType),StructField("empName",StringType)))

    // Create a "inputDir" under the
    val streamDF = spark.readStream.option("header","true").schema(schema).csv("C:\\inputDir")

    val streamDFwithTS = streamDF.withColumn("timestamp" ,unix_timestamp(current_timestamp(), "yyyyMMdd'T'HHmmss:SSSSSS") )

    val query = streamDFwithTS.writeStream.format("console").outputMode(OutputMode.Update()).start()


    query.awaitTermination()

  }

}
