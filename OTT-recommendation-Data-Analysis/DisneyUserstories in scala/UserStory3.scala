
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object UserStory3 extends App{
  // Initialize SparkSession
  val spark = SparkSession.builder()
    .appName("Awards Analysis")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Load the dataset (replace with actual path)
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:/Users/AMSPATIL/OneDrive - Capgemini/Desktop/cleaned_37/disney_cleaned_222.csv")

  // Create a new column for total awards
  val dfWithTotal = df.withColumn("total_awards", col("wins") + col("oscar_wins"))

  // Filter out rows where director is 'Unknown'
  val dfFiltered = dfWithTotal.filter(col("director") =!= "Unknown")

  // Find the year with the highest total awards without using groupBy
  val maxYearRow = dfFiltered
    .select("released_at", "total_awards")
    .na.drop()
    .rdd
    .map(row => (row.getAs[String]("released_at"), row.getAs[Int]("total_awards")))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .first()

  val maxAwardsYear = maxYearRow._1

  // Filter the dataset for that year and get directors with non-zero total awards
  val directorsInMaxYear = dfFiltered
    .filter($"released_at" === maxAwardsYear && $"total_awards" > 0)
    .select("director", "total_awards")

  // Show results
  println(s"The year with the highest total awards is: $maxAwardsYear")
  directorsInMaxYear.show()
}


