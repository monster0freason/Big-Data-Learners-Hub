import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object AmazonUserStory4 extends App {
  val spark= SparkSession.builder()
    .appName("Longest Movie Finder")
    .master("local[*]")
    .getOrCreate()

  val df= spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/amazon_cleaned_data.csv")

  // Find the maximum duration

  val maxDuration= df.agg(max("total_running_time_minutes").alias("max_duration"))
    .collect()(0)
    .getInt(0)

  // Filter movies with that duration
  val longestMoviesDF = df.filter(col("total_running_time_minutes") === maxDuration)
    .select("movie_name", "year_of_release", "total_running_time_minutes")

  longestMoviesDF.show()

  longestMoviesDF.write
    .option("header", "true")
    .csv("data/AmazonUserStory_04")

}
