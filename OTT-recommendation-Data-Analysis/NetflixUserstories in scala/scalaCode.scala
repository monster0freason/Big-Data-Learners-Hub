object netflix_1 extends App{

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._



    // Create a Spark session
    val spark = SparkSession.builder
      .appName("Netflix Analysis")
      .master("local[*]")
      .getOrCreate()



    // Import spark implicits for $"..." syntax
    import spark.implicits._

    // Load the dataset
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/ankimano/Downloads/NetflixCleaned.csv")
      .cache() // It's a good practice to cache the DataFrame if it's used multiple times

    // --- 1. Top Genre for Movies Over 60 Minutes ---
    println("Analysis 1: Top Genre for Movies Over 60 Minutes")
    val moviesOver60min = df.filter($"type" === "Movie" && $"duration".endsWith("min"))
      .withColumn("duration_min", split($"duration", " ").getItem(0).cast("int"))
      .filter($"duration_min" > 60)

    val moviesExploded = moviesOver60min.withColumn("genre", explode(split($"listed_in", ", ")))

    val topGenre = moviesExploded.groupBy("genre")
      .count()
      .orderBy(desc("count"))
      .limit(1)

    topGenre.show()

    // --- 2. Top Country for TV Shows and List Them ---
    println("\nAnalysis 2: Top Country for TV Shows")
    val tvShowsDF = df.filter($"type" === "TV Show")

    val topCountryName = tvShowsDF.groupBy("country")
      .agg(countDistinct("show_id").as("tv_show_count"))
      .orderBy(desc("tv_show_count"))
      .first().getAs[String]("country")

    println(s"Top Country for TV Shows: $topCountryName")

    println(s"\nList of TV Shows from $topCountryName:")
    val topCountryTVShows = tvShowsDF.filter($"country" === topCountryName)
      .select("show_id", "title", "description")

    topCountryTVShows.show(truncate = false)

    // --- 3. Distinct Movie Types Released in 2021 ---
    println("\nAnalysis 3: Distinct Movie Types Released in 2021")
    val movieTypes2021 = df.filter($"type" === "Movie" && $"release_year" === 2021)
      .select("listed_in")
      .distinct()

    movieTypes2021.show(truncate = false)

    // --- 4. Country with the Most Movies ---
    println("\nAnalysis 4: Country with the Most Movies")
    val moviesDF = df.filter($"type" === "Movie")

    val moviesByCountry = moviesDF.withColumn("country", explode(split($"country", ",")))
      .withColumn("country", trim($"country"))

    val topCountryMovies = moviesByCountry.groupBy("country")
      .count()
      .orderBy(desc("count"))
      .first()

    println(s"Country with the most movies: ${topCountryMovies.getAs[String]("country")}, with ${topCountryMovies.getAs[Long]("count")} movies.")

    // --- 5. Country with the Most Comedy Movies ---
    println("\nAnalysis 5: Country with the Most Comedy Movies")
    val comedyMovies = df.filter(
      $"type" === "Movie" &&
        $"listed_in".contains("Comedies") &&
        $"country".isNotNull
    )

    val topComedyCountry = comedyMovies.groupBy("country")
      .count()
      .orderBy(desc("count"))
      .limit(1)

    topComedyCountry.show()

    // Note: spark.stop() is not explicitly called here.
    // The App trait handles termination, but for Spark, an explicit main method and stop() is often safer.


}