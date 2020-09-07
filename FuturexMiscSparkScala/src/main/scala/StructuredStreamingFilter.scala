import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object StructuredStreamingFilter {

  def main(args: Array[String]): Unit = {

    println("Structured Streaming Demo")

    val conf = new SparkConf().setAppName("Spark Structured Streaming").setMaster("local[*]")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    println("Spark Session created")

    val schema = StructType(Array(StructField("phone",StringType),StructField("sale_volume",IntegerType)))

    // Create a "inputDir" under the
    val streamDF = spark.readStream.option("header","true")
      .schema(schema).csv("C:\\inputDir").groupBy("phone").sum("sale_volume")

    streamDF.createOrReplaceTempView("tempview")

    val iPhoneDF = spark.sql("select * from tempview where phone = 'iPhone'")

    val query = iPhoneDF.writeStream.format("console").outputMode(OutputMode.Complete()).start()


    query.awaitTermination()

  }

}
