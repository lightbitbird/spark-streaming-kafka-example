package streaming.structured.cassandra

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, from_json, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types._

case class SensorData(id: Long,
                      coord: Coord,
                      date: String,
                      ts: String,
                      main: Main
                     ) extends Serializable

case class Coord(lon: Double, lat: Double)

case class Main(temperature: Double, humidity: Double, ph: Double, whc: Double)

object StreamingWithCasssandra extends SparkSessionBuilder {

  def main(args: Array[String]): Unit = {

    val spark = buildSparkSession
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val connector = CassandraConnector(spark.sparkContext.getConf)

    def processRow(data: SensorData) = connector.withSessionDo {
      session =>
        session.execute {
          s"""insert into imai_farm.sensor_raw(id, coord, date, ts, temperature, humidity, ph, whc)
           values(${data.id}, {lon:${data.coord.lon}, lat:${data.coord.lat}},
           '${data.date}', '${data.ts}', ${data.main.temperature},
          ${data.main.humidity}, ${data.main.ph}, ${data.main.whc})"""
        }
    }

    val kafkaDataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.100.141:9092")
      .option("subscribe", "sensor-data1").load()

    val stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")
    val coordSchema = StructType(StructField("lat", DoubleType) :: StructField("lon", DoubleType) :: Nil)
    val mainSchema = StructType(StructField("temperature", DoubleType) :: StructField("humidity", DoubleType) :: StructField("ph", DoubleType) :: StructField("whc", DoubleType) :: Nil)
    val schema = StructType(StructField("id", LongType) :: StructField("date", StringType) :: StructField("coord", coordSchema) :: StructField("main", mainSchema) :: Nil)

    val jsonParsedDf = stringFormattedDataFrame.select(from_json($"value", schema).as("sensor_data"))
    val formattedDataFrame = jsonParsedDf.select(jsonParsedDf.col("sensor_data.id").alias("id"),
      jsonParsedDf.col("sensor_data.date").alias("date"),
      jsonParsedDf.col("sensor_data.coord").alias("coord"),
      jsonParsedDf.col("sensor_data.main").alias("main")
    )

    val addedDf = formattedDataFrame.withColumn("ts", from_unixtime(unix_timestamp(formattedDataFrame.col("date"), "yyyy/MM/dd HH:mm:ss")))
    val cassandraDf = addedDf.select(addedDf.col("id"), addedDf.col("coord"),
      date_format(addedDf.col("ts"), "yyyyMMdd")
        .alias("date"), addedDf.col("ts"), addedDf.col("main")
    )
    cassandraDf.printSchema()
    val cassandraDs = cassandraDf.select($"id", $"coord", $"date", $"ts", $"main").as[SensorData]

    import org.apache.spark.sql.ForeachWriter

    val writer = new ForeachWriter[SensorData] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: SensorData): Unit = {
        processRow(value)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val query = cassandraDs.writeStream.queryName("kafkaToCassandraForeach").foreach(writer).start
    query.awaitTermination()

  }

}

trait SparkSessionBuilder {
  def buildSparkSession: SparkSession = {
    lazy val conf: SparkConf = new SparkConf()
      .setAppName("Structured Streaming from Kafka to Cassandra")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")
    lazy val spark = SparkSession.builder()
      .appName("KafkaCassandra")
      .config("spark.master", "local")
      .config(conf)
      .getOrCreate()
    spark
  }
}