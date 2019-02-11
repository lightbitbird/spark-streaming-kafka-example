package streaming.structured.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SlidingWindow {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TimestampConverted")
      .config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val kafkaDataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.100.141:9092")
      .option("subscribe", "joined-sensor-data").load()

    // convert Value columns into strings
    val stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    val schema = StructType(StructField("sensor_id", LongType) :: StructField("field_id", StringType) :: StructField("date", StringType) :: StructField("lat", DoubleType) :: StructField("lon", DoubleType) :: StructField("temperature", DoubleType) :: StructField("humidity", DoubleType) :: StructField("ph", DoubleType) :: StructField("whc", DoubleType) :: Nil)

    val jsonParsedDF = stringFormattedDataFrame.select(from_json($"value", schema).as("sensor_data"))
    // parse DataFrame, assigned the schema, as Json
    val formattedDataFrame = jsonParsedDF.select(jsonParsedDF.col("sensor_data.sensor_id").alias("sensor_id"),
      jsonParsedDF.col("sensor_data.date").alias("date"),
      jsonParsedDF.col("sensor_data.field_id").alias("field_id"),
      jsonParsedDF.col("sensor_data.lat").alias("lat"),
      jsonParsedDF.col("sensor_data.lon").alias("lon"),
      jsonParsedDF.col("sensor_data.temperature").alias("temperature"),
      jsonParsedDF.col("sensor_data.humidity").alias("humidity"),
      jsonParsedDF.col("sensor_data.ph").alias("ph"),
      jsonParsedDF.col("sensor_data.whc").alias("whc")
    )
    //convert date column into timestamp format
    val withTimestamp = formattedDataFrame.withColumn("timestamp", (to_timestamp(formattedDataFrame.col("date"), "yyyy/MM/dd HH:mm:ss")))

    // extract columns for analyzing
    val analyzeBase = withTimestamp.select(
      withTimestamp.col("timestamp"),
      withTimestamp.col("field_id"),
      withTimestamp.col("temperature"),
      withTimestamp.col("humidity"),
      withTimestamp.col("ph"),
      withTimestamp.col("whc")
    )

    // set sliding window
    val windowedAvg = analyzeBase.withWatermark("timestamp", "10 minutes")
      .groupBy(window($"timestamp", "5 minutes", "1 minutes"), analyzeBase.col("field_id"))
      .agg(
        sum("temperature").as("avg_temperature"),
        sum("humidity").as("avg_humidity"),
        sum("ph").as("avg_ph"),
        sum("whc").as("avg_whc")
      )
    val timeExtractedAvg = windowedAvg.select("field_id", "window.start", "window.end", "avg_temperature", "avg_humidity", "avg_ph", "avg_whc")

    //filter aggregation results in avg of whc less than 25.0
    val filteredAvg = timeExtractedAvg.filter(timeExtractedAvg.col("avg_whc") < 25.0)

    // output filter results on console
    val consoleQuery = filteredAvg.writeStream.outputMode("update").format("console").start()

    // output filter results to kafka topic 'whc-less-sensor-data'
    val kafkaQuery = filteredAvg.selectExpr("to_json(struct(*)) AS value")
      .writeStream.format("kafka").outputMode("update")
      .option("kafka.bootstrap.servers", "192.168.100.141:9092")
      .option("topic", "whc-less-sensor-data")
      .option("checkpointLocation", "checkpoint/produce_to_kafka").start()


    // Waits for the termination of `this` query, either by `query.stop()` or by an exception.
    kafkaQuery.awaitTermination()

  }

}
