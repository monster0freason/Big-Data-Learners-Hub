import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, trim}

object AmazonUserStory3 extends App {
  val spark = SparkSession.builder.appName("Amazon Analysis").master("local[*]").getOrCreate()

  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/amazon_cleaned_data.csv")


  // Filter for Hindi movies
  val hindiMoviesDF = df.filter(col("language") === "Hindi")

  // Select distinct combinations of maturity rating and movie name
  val resultDF = hindiMoviesDF.select("maturity_rating", "movie_name").distinct()

  // Show the result
  resultDF.show(false)

  resultDF.write.option("header", "true").mode("overwrite").csv("data/AmazonUserStory_03")

  spark.stop()

}
