package SparkSQLDatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.functions._

object MostPopularMovie extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder.appName("Most Popular Movie").master("local[*]").getOrCreate()

  val movieSchema = new StructType()
    .add("user_id", IntegerType, nullable = true)
    .add("movie_Id", IntegerType, nullable = true)
    .add("rating", IntegerType, nullable = true)
    .add("time_stamp", LongType, nullable = true)


  val movieDf = spark.read.schema(movieSchema).option("sep","\t").csv("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\ml-100k\\ml-100k\\u.data")

  val movieRatingDf = movieDf.select("movie_Id").groupBy(col("movie_Id")).agg(count("movie_Id").as("total_count"))
    .orderBy(col("total_count").desc_nulls_last)

  movieRatingDf.show()


}
