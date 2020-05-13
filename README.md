# WTW Spark Batch

This project uses Apache Spark to perform a batch job with the 
[Group Lens dataset](https://grouplens.org/datasets/movielens/) (We run it using the ml-latest-small.zip dataset specifically)

## Running Application

As pre-requisites you will need to have apache spark installed or have a cluster where you can use spark-sumbmit to send
the jar. 

This saves the data in a postgreSQL DB so you will need to chage the credentials appropiately. 

```
 // PostgreSQL DB data
  val host = "localhost"
  val port = 5432
  val database = "db_name"

  // PostgreSQL User data
  val user = "user"
  val password = "password"
```

The app consumes the [The Movie DB Api](https://developers.themoviedb.org/) so you will need a valid API key to run the 
app as-is. Change the value with your key in `tmdbApi.LiveTmdbClient`.

Assemble the final jar using `assembly` as you will need a fat jar. Run the spark submit providing the postgres driver as follows:

`spark-submit --driver-class-path postgresql-42.2.12.jar --jars postgresql-42.2.12.jar target\scala-2.11\wtw-spark-batch-assembly-0.1.jar`

## Working with the Server

As spark doesn't handle the sql constraints (primary and foreign keys), those need to be added separately. You can run
the following script to be consistent with that the server expects 

```
ALTER TABLE movies ADD PRIMARY KEY (id);
ALTER TABLE genres ADD PRIMARY KEY (id);
ALTER TABLE movie_genres ADD PRIMARY KEY (movie_id, genre_id);
ALTER TABLE movie_genres ADD CONSTRAINT mgfk_movie FOREIGN KEY (movie_id) REFERENCES movies (id);
ALTER TABLE movie_genres ADD CONSTRAINT mgfk_genre FOREIGN KEY (genre_id) REFERENCES genres (id);
```