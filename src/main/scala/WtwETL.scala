import java.sql.Date

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import tmdbApi.{ApiMovie, TmdbApi}

object WtwETL {

  val movieCsvSchema: StructType = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("genres", StringType, nullable = false)
  ))
  val linksCsvSchema: StructType = StructType(Array(
    StructField("movie_id", IntegerType, nullable = false),
    StructField("imdb_id", IntegerType, nullable = true),
    StructField("tmdb_id", IntegerType, nullable = true)
  ))

  case class CsvMovie(id: Int, title: String, genres: String)
  case class LinkedMovie(id: Int, title: String, genres: String, tmdb_id: Option[Int])
  case class DbMovie(id: Int, tmdb_id: Option[Int], title: String, release_date: Date, budget: Option[Int], poster_url: Option[String])
  object DbMovie {

    val BASE_IMAGE_URL = "https://image.tmdb.org/t/p"
    val IMAGE_SIZE = "w500"

    def cleanTitle(rawTitle: String): (String, Option[String]) = {
      val trimmed = rawTitle.trim.stripPrefix("\"").stripSuffix("\"")
      val yearRegex = "\\((\\d+|–)*\\)$".r
      val regexMatch = yearRegex.findFirstIn(trimmed)
      regexMatch.fold({ (trimmed, Option.empty[String]) }) { m =>
        (trimmed.replaceAll(yearRegex.regex, "").trim,
          {
            val year = m.stripPrefix("(").stripSuffix(")")
            if (year.contains("–")) Some(s"${year.substring(0,4)}-01-01")
            else Some(s"$year-01-01")
          })
      }
    }

    def apply(linkedMovie: LinkedMovie, apiMovie: ApiMovie): DbMovie = {
      val titleAndDate = cleanTitle(linkedMovie.title)
      new DbMovie(
        id = linkedMovie.id,
        tmdb_id = linkedMovie.tmdb_id,
        title = titleAndDate._1,
        release_date = Date.valueOf(apiMovie.release_date),
        budget = if (apiMovie.budget > 0) Option(apiMovie.budget) else None,
        poster_url = apiMovie.poster_path.map { path =>
          s"$BASE_IMAGE_URL/$IMAGE_SIZE$path"
        })
    }

    def apply(linkedMovie: LinkedMovie): DbMovie = {
      val titleAndDate = cleanTitle(linkedMovie.title)
      new DbMovie(
        id = linkedMovie.id,
        tmdb_id = linkedMovie.tmdb_id,
        title = titleAndDate._1,
        release_date = titleAndDate._2.fold[Date](null)(Date.valueOf),
        budget = None,
        poster_url = None
      )
    }
  }

  case class MovieGenre(movieId: Int, genre: String)

  // PostgreSQL DB data
  val host = "localhost"
  val port = 5432
  val database = "wtw"

  // PostgreSQL User data
  val user = "user"
  val password = "password"
  
  def postgresUrl(host: String, port: Int, database: String) = s"jdbc:postgresql://$host:$port/$database"

  val movieCsvPath = "C:\\Users\\jtroconis\\datasets\\ml-latest-small\\movies.csv"
  val linksCsvPath = "C:\\Users\\jtroconis\\datasets\\ml-latest-small\\links.csv"

  def readCsv(path: String, schema: StructType)(implicit spark: SparkSession): DataFrame = spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(path)

  def joinMovieAndLinks(movieDf: DataFrame, linksDf: DataFrame): Dataset[LinkedMovie] = {
    import movieDf.sparkSession.implicits._
      movieDf.join(linksDf, $"id" === $"movie_id")
        .select($"id", $"title", $"genres", $"tmdb_id")
        .as[LinkedMovie]
  }

  def mapLinkedMoviesWithApi(linkedMovies: Dataset[LinkedMovie]): Dataset[DbMovie] = {
    import linkedMovies.sparkSession.implicits._
    linkedMovies.map { movie =>
      val api = TmdbApi.raw
      movie.tmdb_id match {
        case Some(tmdb_id) =>
          api.getMovie(tmdb_id) match {
            case Left(err) =>
              println(err.getMessage)
              DbMovie(movie)
            case Right(apiMovie) =>
              val processed = DbMovie(movie, apiMovie)
              println(s"Processed response to $processed")
              processed
          }
        case None => DbMovie(movie)
      }

    }
  }

  def writeDFToPostgres(dataFrame: DataFrame, tableName: String): Unit = dataFrame.write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", postgresUrl(host, port, database))
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName)
      .save()


  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession =
      SparkSession.builder().appName("WTW-ETL").master("local[*]").getOrCreate()

    import spark.implicits._

    val moviesCsvDf = readCsv(movieCsvPath, movieCsvSchema)
    val linksDf = readCsv(linksCsvPath, linksCsvSchema)

    val moviesGenresDf = moviesCsvDf.as[CsvMovie].flatMap { csvMovie =>
      csvMovie.genres.split("\\|")
        .map(genre => MovieGenre(csvMovie.id, genre))
    }.toDF("movie_id", "genre").cache()

    import org.apache.spark.sql.functions._
    val genresDf = moviesGenresDf
      .select($"genre")
      .distinct()
      .withColumn("id", monotonically_increasing_id()).cache()
    writeDFToPostgres(genresDf, "genres")

    val movieGenreIdDf = moviesGenresDf
      .join(genresDf.withColumnRenamed("id", "genre_id"), moviesGenresDf("genre") === genresDf("genre"))
      .select($"movie_id", $"genre_id")
    writeDFToPostgres(movieGenreIdDf, "movie_genres")

    val joinedDf = joinMovieAndLinks(moviesCsvDf, linksDf)
    val dbMovies = mapLinkedMoviesWithApi(joinedDf)
    writeDFToPostgres(dbMovies.toDF("id", "tmdb_id", "title", "release_date", "budget", "poster_url"), "movies")
  }
}
