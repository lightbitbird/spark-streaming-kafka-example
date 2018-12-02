package streaming.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.types._

object ParquetOutput {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaToParquet")
      .config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val kafkaDataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.100.141:9092")
      .option("subscribe", "sensor-data1").load()

    val stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    val coordSchema = StructType(StructField("lat", DoubleType) :: StructField("lon", DoubleType) :: Nil)
    val mainSchema = StructType(StructField("temperature", DoubleType) :: StructField("humidity", DoubleType) :: StructField("ph", DoubleType) :: StructField("whc", DoubleType) :: Nil)
    val schema = StructType(StructField("id", LongType) :: StructField("date", StringType) :: StructField("coord", coordSchema) :: StructField("main", mainSchema) :: Nil)

    val jsonParsedDF = stringFormattedDataFrame.select(from_json($"value", schema).as("sensor_data"))
    val formattedDataFrame = jsonParsedDF.select(jsonParsedDF.col("sensor_data.id").alias("id"),
      jsonParsedDF.col("sensor_data.date").alias("date"),
      jsonParsedDF.col("sensor_data.coord.lat").alias("lat"),
      jsonParsedDF.col("sensor_data.coord.lon").alias("lon"),
      jsonParsedDF.col("sensor_data.main.temperature").alias("temperature"),
      jsonParsedDF.col("sensor_data.main.humidity").alias("humidity"),
      jsonParsedDF.col("sensor_data.main.ph").alias("ph"),
      jsonParsedDF.col("sensor_data.main.whc").alias("whc")
    )

    val withMonth = formattedDataFrame.withColumn("ts", from_unixtime(unix_timestamp(formattedDataFrame.col("date"), "yyyy/MM/dd HH:mm:ss")))
      .select(date_format(formattedDataFrame.col("date"), "yyyyMM").alias("month"), formattedDataFrame.col("*"))

    val query = withMonth.writeStream.outputMode("append")
      .format("parquet")
      .option("path", "./src/main/scala/streaming/structured/parquetPartitioned")
      .option("checkpointLocation", "./src/main/scala/streaming/structured/state/KafkaToParquetFile")
      .start()

    query.awaitTermination()

  }

}
