
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserStory4 extends App{
  // Initialize SparkSession
  val spark = SparkSession.builder()
    .appName("Disney Nominations Analysis")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Load your dataset (replace with actual path)
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:/Users/AMSPATIL/OneDrive - Capgemini/Desktop/cleaned_37/disney_cleaned_222.csv")

  // Add a new column for total nominations
  val df_with_total_nominations = df.withColumn("total_nominations",
    col("nominations") + col("oscar_nominations") + col("golden_globe_nominations"))

  // Extract year from 'released_at' and convert to full year

  val df_with_year = df_with_total_nominations
    .withColumn("release_year",
      expr("cast(regexp_extract(released_at, '(\\\\d{2})-(\\\\w{3})-(\\\\d{2})', 3) as int) + 1900"))
    .withColumn("release_year",
      expr("release_year + (100 * cast(release_year < 1950 as int))"))


  // Filter for nominated movies between 1998 and 2018
  val nominated_movies = df_with_year.filter(
    col("release_year").between(1998, 2018) &&
      col("total_nominations").isNotNull &&
      col("total_nominations") > 0
  )

  // Count the total number of such movies
  val total_nominated = nominated_movies.count()

  println(s"Total number of Disney movies nominated for awards between 1998 and 2018: $total_nominated")

  spark.stop()

}
