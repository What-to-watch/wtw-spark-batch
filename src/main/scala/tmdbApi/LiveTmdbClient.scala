package tmdbApi

import sttp.client._
import sttp.client.SttpBackend
import sttp.client.circe._
import io.circe.generic.auto._

import scala.concurrent.duration.Duration

//class LiveTmdbClient(client: SttpBackend[Task, Nothing, WebSocketHandler]) extends TmdbApi.Service {
//
//  import LiveTmdbClient._
//
//  override def getMovie(id: Int): Task[ApiMovie] = {
//    val request = basicRequest
//      .get(uri"$API_BASE_URL/movie/$id?api_key=$API_KEY&append_to_response=images")
//      .response(asJson[ApiMovie])
//    client.send(request)
//      .map(_.body)
//      .flatMap(ZIO.fromEither(_))
//  }
//}

@SerialVersionUID(100L)
class LiveTmdbClient(client: SttpBackend[Identity, Nothing, NothingT]) extends TmdbApi.Service with Serializable {

  HttpURLConnectionBackend()
  import LiveTmdbClient._

  override def getMovie(id: Int): Either[Throwable, ApiMovie] = {
    println(s"$API_BASE_URL/movie/$id?api_key=$API_KEY")
    val request = basicRequest
      .get(uri"$API_BASE_URL/movie/$id?api_key=$API_KEY")
      .response(asJson[ApiMovie])
      .readTimeout(Duration.Inf)
    val response = client.send(request).body
    println(s"Got response with response: $response")
    response
  }
}

object LiveTmdbClient {
  val API_KEY = "808cd9823f83744a8ebd68dd4664e076"
  val API_BASE_URL = "https://api.themoviedb.org/3"

  def apply(client: SttpBackend[Identity, Nothing, NothingT]): LiveTmdbClient = new LiveTmdbClient(client)

}
