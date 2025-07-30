import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserStory2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Movies Released in 1999 by Director")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load your DataFrame here (replace with actual data source)
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/disney_cleaned_222.csv")

    // Filter movies released in 1999
    val movies_1999 = df.filter(col("released_at").rlike("(?i)-99$"))

    // Group by director and count the number of movies
    val movies_by_director = movies_1999.groupBy("director").count()

    // Show the result
    movies_by_director.show()

    spark.stop()


  }


}
