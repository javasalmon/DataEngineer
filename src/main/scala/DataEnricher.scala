import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataEnricher {
  
  def main(args: Array[String]) {
    val userHome = System.getProperty("user.home");

    val spark = SparkSession.builder.appName("New Day Homework Application").getOrCreate()

    //Movie Schema
    val movieSchema = StructType(
      Array(
        StructField("movieId",  IntegerType, true),
        StructField("empty1",   StringType,  true),
        StructField("title",    StringType, true),
        StructField("empty2",   StringType,  true),
        StructField("genres",   StringType, true)
      )
    )

    //Read movies data - Task 1
    val movies = spark.read.option("delimiter", ":")
      .schema(movieSchema)
      .csv(userHome.concat("/tmp/ml-1m/movies.dat"))

    //Rating Schema
    val ratingSchema = StructType(
      Array(
        StructField("userId",   IntegerType, true),
        StructField("empty1",   StringType,  true),
        StructField("movieId",  IntegerType, true),
        StructField("empty2",   StringType,  true),
        StructField("rating",   IntegerType, true),
        StructField("empty3",   StringType,  true),
        StructField("ratingTs", StringType,  true)
      )
    )
    //Read ratings data - Task 1
    val ratings = spark.read.option("delimiter", ":")
      .schema(ratingSchema)
      .csv(userHome.concat("/tmp/ml-1m/ratings.dat"))
      .withColumnRenamed("movieId", "r_movieId")

    //User schema
    val userSchema = StructType(
      Array(
        StructField("userId",   IntegerType, true),
        StructField("empty1",   StringType,  true),
        StructField("gender",   StringType, true),
        StructField("empty2",   StringType,  true),
        StructField("age",      IntegerType, true),
        StructField("empty3",   StringType,  true),
        StructField("occupation", StringType,  true),
        StructField("empty4",   StringType,  true),
        StructField("zipCode",  StringType,  true)
      )
    )

    //Read users data
    val users = spark.read.option("delimiter", ":")
      .schema(userSchema)
      .csv(userHome.concat("/tmp/ml-1m/users.dat"))

    import org.apache.spark.sql.functions.{min, max, avg, col, row_number, rank}

    //Inner join users with ratings
    val userRatingsDF = users.join(ratings,users("userId") === ratings("userId"),"inner" )
      .groupBy("r_movieId")
      .agg(
        max("rating").as("MaximumRating"),
        min("rating").as("MinimumRating"),
        avg("rating").as("AverageRating")
      );

    //Inner join user ratings with movies - Task 2
    val movieRatings = movies.join(userRatingsDF,movies("movieId") === userRatingsDF("r_movieId"),"inner" )
      .drop("empty1", "empty2", "empty3", "r_movieId")

    //Create Window spec to find top three movies based on their rating - Task 3
    import org.apache.spark.sql.expressions.Window
    val windowSpec = Window.partitionBy("userId")
      .orderBy(col("rating").desc)

    val usersTopThreeMoviesByRatingDF = movies.join(ratings,movies("movieId") === ratings("r_movieId"),"inner" )
      .withColumn("row",row_number.over(windowSpec))
      .where(col("row") <= 3 )
      .drop("row", "empty1", "empty2", "empty3", "r_movieId")
      .orderBy(col("userId").asc)


    val uuid = java.util.UUID.randomUUID

    //Writes out the original dataframes - Task 4

    //Write Ratings in parquet format - Task 4
    ratings.withColumnRenamed("r_movieId","movieId")
      .write.parquet(userHome.concat(s"/tmp/nw/output/${uuid}/ratings.parquet"))

    //Write Movies in parquet format
    movies.write.parquet(userHome.concat(s"/tmp/nw/output/${uuid}/movies.parquet"))

    //Write Users in parquet format
    users.write.parquet(userHome.concat(s"/tmp/nw/output/${uuid}/users.parquet"))

    //Write movieRatings dataframes in parquet format- Task 4
    movieRatings.write.parquet(userHome.concat(s"/tmp/nw/output/${uuid}/movieRatings.parquet"))

    //Write usersTopThreeMoviesByRatingDF dataframes in parquet format- Task 4
    usersTopThreeMoviesByRatingDF.write.parquet(userHome.concat(s"/tmp/nw/output/${uuid}/usersTopThreeMoviesByRatingDF.parquet"))


    println(s"Old and new dataframes written to ${userHome}/tmp/nw/output/${uuid} in parquet format")

    spark.stop()
  }
}