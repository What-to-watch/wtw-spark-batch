package tmdbApi

case class ApiMovie(id: Int,
                     budget: Int,
                     poster_path: Option[String],
                     release_date: String
                   )

case class Poster(
                 file_path: String,
                 aspect_ratio: Int,
                 height: Int,
                 width: Int
                 )
