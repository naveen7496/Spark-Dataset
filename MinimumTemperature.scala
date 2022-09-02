package SparkSQLDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._


object MinimumTemperature extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Temperature(stationId :String, date : Int, measure_type : String, temperature : Float)

  val spark = SparkSession.builder.appName("Word Counter").master("local[*]").getOrCreate()

  import spark.implicits._

  val tempSchema = new StructType()
    .add("stationId", StringType, nullable = true)
    .add("date", IntegerType, nullable = true)
    .add("measure_type", StringType, nullable = true)
    .add("temperature", FloatType, nullable = true)

  val input = spark.read.schema(tempSchema).csv("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\1800.csv").as[Temperature]

  val minTemperatureData = input.select("stationId","measure_type","temperature").where(col("measure_type") === "TMIN")

  val minTempByStation = minTemperatureData.groupBy(col("stationId")).agg(min("temperature").as("min_temp"))

  minTempByStation.show()



}
