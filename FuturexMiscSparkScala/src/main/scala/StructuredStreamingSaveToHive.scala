import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object StructuredStreamingSaveToHive {

  def main(args: Array[String]): Unit = {

    println("Structured Streaming Demo")

    val conf = new SparkConf().setAppName("Spark Structured Streaming").setMaster("local[*]")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    println("Spark Session created")

    val schema = StructType(Array(StructField("empId",StringType),StructField("empName",StringType)))

    // Create a "inputDir" under the
    val streamDF = spark.readStream.option("header","true").schema(schema).csv("C:\\inputDir")

    val query = streamDF.writeStream.outputMode(OutputMode.Append()).format("csv")
      .option("path","hivelocation").option("checkpointLocation","location").start()

    query.awaitTermination()

  }

}
