package SparkSQLDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}
import org.apache.spark.sql.functions._

object CustomerOrder extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Order(user_id :Int, item_id : Float, price : Float)

  val spark = SparkSession.builder.appName("Word Counter").master("local[*]").getOrCreate()

  import spark.implicits._

  val orderSchema = new StructType()
    .add("user_id", IntegerType, nullable = true)
    .add("item_id", FloatType, nullable = true)
    .add("price", FloatType, nullable = true)

  val input = spark.read.format("csv").schema(orderSchema).load("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\customer-orders.csv").as[Order]

  val filteredData = input.select("user_id","price").groupBy(col("user_id"))
    .agg(sum("price").as("total_amount_spent")).orderBy(col("total_amount_spent").desc_nulls_last)
  filteredData.show()
}
