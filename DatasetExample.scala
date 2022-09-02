package SparkSQLDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._

object DatasetExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(id:Int, name : String, age : Int, friends : Int)

  val spark = SparkSession.builder.appName("Dataset Practice").master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val fakeFriends = spark.read.option("header","true").option("inferSchema","true")
    .csv("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\fakefriends.csv")
    .as[Person]

//  fakeFriends.printSchema()
  fakeFriends.createOrReplaceTempView("people")

  val teenagers = spark.sql("SELECT *, ROW_NUMBER() OVER(ORDER BY id) AS rn FROM people WHERE age > 12 AND age < 19")
  val adultsUnderThirty =  fakeFriends.select("*").where(col("age") > 18 and col("age") < 30)
//  adultsUnderThirty.show()

  val fakeFriendsFiltered = fakeFriends.select("age","friends")
  val avgNumberFriendsByAge = fakeFriendsFiltered.groupBy(col("age"))
    .agg(round(avg("friends"),2).as("total_friends"))
    .sort(col("total_friends").desc_nulls_last)
  avgNumberFriendsByAge.show()

  spark.stop()



}
