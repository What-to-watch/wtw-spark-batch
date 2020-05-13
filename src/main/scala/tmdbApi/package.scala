import sttp.client.{HttpURLConnectionBackend, Identity, NothingT, SttpBackend, SttpBackendOptions}
import sttp.client.okhttp.OkHttpSyncBackend
import zio.{Has, RIO, RLayer, Task, ZIO, ZLayer}

import scala.concurrent.duration._

package object tmdbApi {

  type TmdbApi = Has[TmdbApi.Service]

  object TmdbApi {
    trait Service {
      def getMovie(id: Int): Either[Throwable, ApiMovie]
    }

    val live: RLayer[Has[SttpBackend[Identity, Nothing, NothingT]], TmdbApi] = ZLayer.fromFunction { client =>
      LiveTmdbClient(client.get)
    }

    val raw: TmdbApi.Service = LiveTmdbClient(OkHttpSyncBackend(SttpBackendOptions.connectionTimeout(10.minutes)))
  }

  def getService: RIO[TmdbApi, TmdbApi.Service] =
    ZIO.access(_.get)
}
