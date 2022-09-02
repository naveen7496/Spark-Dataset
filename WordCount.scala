package SparkSQLDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Book(value :String)

  val spark = SparkSession.builder.appName("Word Counter").master("local[*]").getOrCreate()

  import spark.implicits._

  val input = spark.read.text("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\book.txt").as[Book]

  val splitData  = input.select(explode(split($"value", "\\W+")).as("word")).filter(col("word") =!= "")

  val lowerCaseWords = splitData.select(lower($"word").as("word"))

  val wordCount = lowerCaseWords.groupBy(col("word")).agg(count("*").as("total_no_words")).sort(col("total_no_words").desc_nulls_last)

  wordCount.show()
}
