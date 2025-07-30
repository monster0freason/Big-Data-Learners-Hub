//Review the movie names and languages names in the year of 2019.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object AmazonUserStory1 extends App {
  val spark = SparkSession.builder.appName("Amazon Analysis").master("local[*]").getOrCreate()

  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/amazon_cleaned_data.csv")

  val movies2019DF = df.filter(col("year_of_release") === "2019").select("movie_name", "language")

  movies2019DF.show(50)
}
