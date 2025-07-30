import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, trim}

object AmazonUserStory5 extends App {
  val spark= SparkSession.builder()
    .appName("Longest Movie Finder")
    .master("local[*]")
    .getOrCreate()

  val df= spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/amazon_cleaned_data.csv")

    // Filter out rows with null maturity rating
    val validRatingsDF = df.filter(col("maturity_rating").isNotNull)

    // 1. Total number of movie ratings by language
    val ratingsByLanguageDF = validRatingsDF
      .groupBy("language")
      .agg(count("maturity_rating").alias("total_ratings"))

    ratingsByLanguageDF.orderBy(col("total_ratings").desc).show(false)

  ratingsByLanguageDF
    .orderBy(col("total_ratings").desc)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv("data/AmazonUserStory5_01")


  // 2. Movies with high maturity rating
    val highMaturityLevels = Seq("18+", "R", "NC-17")
    val highMaturityMoviesDF = validRatingsDF
      .filter(trim(col("maturity_rating")).isin(highMaturityLevels: _*))
      .select("movie_name", "maturity_rating")
      .na.drop()

  highMaturityMoviesDF.show(false)

  highMaturityMoviesDF
    .write
    .option("header", "true")
    .mode("overwrite")
    .parquet("data/AmazonUserStory5_02")

  spark.stop()
}
