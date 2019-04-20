package com.andersenlab

import akka.http.scaladsl._
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object PseudoGeocodeServer extends Directives {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("geocode")
    implicit val mat = ActorMaterializer()
    import system.dispatcher

    val countries = Vector("USA", "Ukraine", "Russia", "Belarus", "France", "Egypt", "UK")
    val server = path("geocode") {
      parameters('latitude.as[Double], 'longitude.as[Double]) { (lat, lon) =>
        complete(
          countries(
            (lat + lon).toInt % countries.size
          )
        )
      }
    }

    val bind = Http().bindAndHandle(server, "0.0.0.0", 8080)

    sys.addShutdownHook {
      Await.result(for {
        b <- bind
        _ <- b.unbind()
        _ <- system.terminate()
      } yield (), Duration.Inf)
    }
  }
}
