package streaming.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, struct}
import org.apache.spark.sql.types._

object StreamingWithKafka {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("KafkaConsole")
      .config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val kafkaDataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.100.141:9092")
      .option("subscribe", "sensor-data1").load()

    val stringFormatDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) AS value")
    val coordSchema = StructType(StructField("lat", DoubleType) :: StructField("lon", DoubleType) :: Nil)
    val mainSchema = StructType(StructField("temperature", DoubleType) :: StructField("humidity", DoubleType) :: StructField("ph", DoubleType) :: StructField("whc", DoubleType) :: Nil)
    val schema = StructType(StructField("id", LongType) :: StructField("date", StringType) :: StructField("coord", coordSchema) :: StructField("main", mainSchema) :: Nil)

    val jsonParsedDataFrame = stringFormatDataFrame.select(from_json($"value", schema).as("sensor_data"))
    val formattedDataFrame = jsonParsedDataFrame.select(struct("sensor_data.id").alias("id"),
      struct("sensor_data.coord.lat").alias("lat"),
      struct("sensor_data.coord.lon").alias("lon"),
      struct("sensor_data.main.temperature").alias("temperature"),
      struct("sensor_data.main.humidity").alias("humidity"),
      struct("sensor_data.main.ph").alias("ph"),
      struct("sensor_data.main.whc").alias("whc")
    )

    val query = formattedDataFrame.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
  }

}
