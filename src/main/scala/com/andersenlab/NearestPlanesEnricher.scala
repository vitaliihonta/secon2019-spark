package com.andersenlab

import java.util.Locale
import org.apache.spark.sql.{Dataset, SparkSession}

object NearestPlanesEnricher {
  def main(args: Array[String]): Unit = {
    Locale.setDefault(Locale.UK)

    val spark = SparkSession.builder()
      .appName("VelocityEnricher")
      .master("local[*]")
      .getOrCreate()

    try {
      impl(spark)
    } finally {
      spark.stop()
    }
  }

  private def impl(spark: SparkSession): Unit = {

    import spark.implicits._

    val planeData: Dataset[PlaneDataWithVelocities] =
      Kafka.read(spark)(bootstrapServers = "kafka:9092", topic = "plane_data_with_velocities", schema = Schemas.PlaneEventWithVelocities)
        .as[PlaneDataWithVelocities]


    val SecondWindow = 1000

    def distance(x1: Double, x2: Double, y1: Double, y2: Double, z1: Double, z2: Double): Double =
      math.sqrt(math.pow(x2 - x1, 2) + math.pow(y2 - y1, 2) + math.pow(z2 - z1, 2))

    val dataWithNearestPlanes = planeData
      .groupByKey(_.timestamp / SecondWindow)
      .flatMapGroups { (fiveMinsWindow, windowedEvents) =>
        val windowedEventsStream = windowedEvents.toVector
        windowedEventsStream.map { event =>
          val nearest5Planes = windowedEventsStream
            .filterNot(_.planeId == event.planeId)
            .map(event2 => distance(event.x, event2.x, event.y, event2.y, event.z, event2.z) -> event2.planeId)
            .sortBy(_._1)
            .take(5)
            .toSet

          PlaneDataWithNearestPlanes(event.planeId, event.x, event.y, event.z, event.timestamp, event.velocity, event.avgVelocity, event.maxVelocity, event.minVelocity, nearest5Planes)
        }
      }


    Kafka.write(dataWithNearestPlanes)(bootstrapServers = "localhost:9092", "plane_data_with_addresses", "checkpoint_nearest_planes")
      .awaitTermination()

  }
}
