package SparkSQLDatasets



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}


object MostPopularMovieWithBroadcastJoin extends App {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  def loadMoviesName(): Map[Int, String] ={
    implicit val codec : Codec = Codec("ISO-8859-1")

   var moviesNames: Map[Int,String] = Map()

    val lines = Source.fromFile("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\ml-100k\\ml-100k\\u.item")
    for( line <- lines.getLines()){
      val fields = line.split("|")
      if (fields.length > 1){
        moviesNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    moviesNames
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder.appName("Most Popular Movie").master("local[*]").getOrCreate()

  import spark.implicits._

  val movieSchema = new StructType()
    .add("user_id", IntegerType, nullable = true)
    .add("movieID", IntegerType, nullable = true)
    .add("rating", IntegerType, nullable = true)
    .add("time_stamp", LongType, nullable = true)

  val nameDict = spark.sparkContext.broadcast(loadMoviesName())

  val lookupName : Int => String = (movieID:Int)=>{
    nameDict.value(movieID)
  }

  val lookupNameUDF = udf(lookupName)

  val movieDf = spark.read.schema(movieSchema).option("sep","\t").csv("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\ml-100k\\ml-100k\\u.data")
    .as[Movies]
//  val movieRatingDf = movieDf.select("movieID").groupBy(col("movieID")).agg(count("movieID").as("total_count"))
//    .orderBy(col("total_count").desc_nulls_last)
  val movieCounts = movieDf.groupBy("movieID").count()

//  movieRatingDf.show()
//  movie_Id

  val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))
  moviesWithNames.show()

}
