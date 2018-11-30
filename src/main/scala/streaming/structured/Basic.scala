package streaming.structured

import java.net.InetAddress

import org.apache.spark.sql.SparkSession

object Basic {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkStructuredStreaminNetworkWordcount")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val lines = spark.readStream.format("socket")
      .option("host", InetAddress.getLocalHost.getHostAddress)
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

  }

}
