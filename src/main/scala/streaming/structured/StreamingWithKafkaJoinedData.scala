package streaming.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

object StreamingWithKafkaJoinedData {
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

    val jsonParsedDf = stringFormatDataFrame.select(from_json($"value", schema).as("sensor_data"))
    val formattedDataFrame = jsonParsedDf.select(jsonParsedDf.col("sensor_data.id").alias("id"),
      jsonParsedDf.col("sensor_data.date").alias("date"),
      jsonParsedDf.col("sensor_data.coord.lat").alias("lat"),
      jsonParsedDf.col("sensor_data.coord.lon").alias("lon"),
      jsonParsedDf.col("sensor_data.main.temperature").alias("temperature"),
      jsonParsedDf.col("sensor_data.main.humidity").alias("humidity"),
      jsonParsedDf.col("sensor_data.main.ph").alias("ph"),
      jsonParsedDf.col("sensor_data.main.whc").alias("whc")
    )

    // change the column `id` into `sensor_id`
    val columnRenamedDataFrame = formattedDataFrame.withColumnRenamed("id", "sensor_id")
    val masterSchema = StructType(StructField("sensor_id", LongType) :: StructField("field_id", StringType) :: Nil)

    val sensorMasterDataFrame = spark.read.format("com.databricks.spark.csv")
      .schema(masterSchema)
      .option("header", "true")
      .load("./src/main/resources/sensor_field.csv")

    val joinedDataFrame = columnRenamedDataFrame.join(sensorMasterDataFrame, columnRenamedDataFrame("sensor_id") === sensorMasterDataFrame("sensor_id"), "left_outer")

//    val query = joinedDataFrame.writeStream.outputMode("append").format("console").start()
    val query = joinedDataFrame.selectExpr("to_json(struct(*)) AS value")
      .writeStream.format("kafka").outputMode("update")
      .option("kafka.bootstrap.servers", "192.168.100.141:9092")
      .option("topic", "joined-sensor-data")
      .option("checkpointLocation", "./src/main/scala/streaming/structured/state/JoinSensorToKafka")
      .start()

    query.awaitTermination()
  }

}
