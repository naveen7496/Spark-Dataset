package SparkSQLDatasets

import com.sundogsoftware.spark.MostPopularSuperheroDataset.SuperHero
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

object MostPopularSuperHero extends App{

  case class SuperHero(value : String)
  case class SuperHeroName(id : Int, name : String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder.appName("Most popular SuperHero").master("local[*]").getOrCreate()

  val superHeroNameSchema = new StructType()
    .add("id", IntegerType, nullable = true)
    .add("name", StringType, nullable = true)

  import spark.implicits._

  val superHeroNamesDs = spark.read.schema(superHeroNameSchema).option("sep"," ")
    .csv("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\Marvel-names.txt").as[SuperHeroName]

  val lines = spark.read
    .text("D:\\SparkScalaCourse\\SparkScalaCourse\\data\\Marvel-graph.txt")
    .as[SuperHero]


  val mostPopularId = lines.withColumn("id", split(col("value"), " ")(0))
                          .withColumn("connections", size(split(col("value"), " ")) - 1)
                          .groupBy(col("id")).agg(sum(col("connections")).as("total_connections"))
                          .orderBy(col("total_connections").desc_nulls_last).first()


  val connections = lines.withColumn("id", split(col("value"), " ")(0))
    .withColumn("connections", size(split(col("value"), " ")) - 1)
    .groupBy(col("id")).agg(sum(col("connections")).as("total_connections"))
    .orderBy(col("total_connections").desc_nulls_last)

//    connections.show()

  val superHeroNames = connections.join(superHeroNamesDs, connections.col("id") === superHeroNamesDs.col("id"))

  superHeroNames.show()

  val mostPopularSuperHeroName = superHeroNamesDs.filter(col("id") === mostPopularId(0))
                                .select(col("name")).first()

//  println(mostPopularSuperHeroName)



}
