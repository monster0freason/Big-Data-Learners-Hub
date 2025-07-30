
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object UserStory1 {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("Top Director Analysis")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      // Load your dataset into a DataFrame (replace with actual data loading logic)
      val disdf = spark.read.option("header", "true").csv("C:/Users/AMSPATIL/OneDrive - Capgemini/Desktop/cleaned_37/disney_cleaned_222.csv")

      // Step 1: Group by director and sum the votes
      val directorVotes = disdf
        .groupBy("director")
        .agg(sum("imdb_votes").alias("total_votes"))

      // Step 2: Find the director with the highest total votes
      val topDirectorRow = directorVotes
        .orderBy(col("total_votes").desc)
        .first()

      val topDirector = topDirectorRow.getString(0)

      // Step 3: Filter the original dataset for the top director (without lit)
      val topDirectorDF = disdf.filter($"director" === topDirector)

      // Step 4: Group by year and sum the votes for the top director
      val yearlyVotes = topDirectorDF
        .groupBy("added_at")
        .agg(sum("imdb_votes").alias("yearly_votes"))

      // Step 5: Find the year with the highest votes for the top director
      val topYearRow = yearlyVotes
        .orderBy(col("yearly_votes").desc)
        .first()

      val topYear = topYearRow.getString(0)
      val topYearVotes = topYearRow.getDouble(1).toLong


      // Final Output
      println(s"The director with the highest total votes is $topDirector.")
      println(s"The year with the highest votes for $topDirector is $topYear with $topYearVotes votes.")

      spark.stop()
    }

}
