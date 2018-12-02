package streaming.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object UdfRegister {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("ConfirmRegisterUDF")
      .config("spark.master", "local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.udf.register("doubleString", (str: String) => str + str)

    val masterSchema = StructType(StructField("sensor_id", LongType) :: StructField("field_id", StringType) :: Nil)

    val sensorMasterDataFrame = spark.read.format("com.databricks.spark.csv")
      .schema(masterSchema)
      .option("header", "true")
      .load("./src/main/resources/sensor_field.csv")

    sensorMasterDataFrame.createOrReplaceTempView("sensor_master")

    val udfAppliedDataFrame = spark.sql("select sensor_id, field_id, doubleString(field_id) as double_field_id from sensor_master")

    udfAppliedDataFrame.printSchema()
    udfAppliedDataFrame.show()

  }

  val doubleString = (str: String) => {
    str + str
  }

}
