//Which are the languages, movies highly watched by people of all ages?
// And in this scenrio, give the movie names of that language
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, trim, lit}

object AmazonUserStory2 extends App{
  val spark = SparkSession.builder.appName("Amazon Analysis").master("local[*]").getOrCreate()

  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/amazon_cleaned_data.csv")

  // Filter movies with 'All' maturity rating
  val allAgesDF = df.filter(trim(col("maturity_rating")) === "All")

  // Count movies by language
  val languageCountsDF = allAgesDF.groupBy("language").count().cache()

  // Get the most common language
  val topLanguage = languageCountsDF.orderBy(col("count").desc).limit(1).collect()(0).getString(0)

  // Filter movies in the top language
  val topLanguageMoviesDF = allAgesDF.filter(col("language") === lit(topLanguage)).select("movie_name")

  // Show results
  println(s"Most common language for 'All' rated movies: $topLanguage")
  topLanguageMoviesDF.show(false)
  // Save the output to a CSV file
  topLanguageMoviesDF.write
    .option("header", "true")
    .mode("overwrite")
    .csv("data/AmazonUserStory_02")

  spark.stop()

}
