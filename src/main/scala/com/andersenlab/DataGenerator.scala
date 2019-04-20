package com.andersenlab

import java.time.Instant
import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization.write
import scala.util.Random

object DataGenerator {
  private def eventsStream = Stream.continually(Random.nextInt(10)).map { id =>
    PlaneData.random(id, Instant.now().toEpochMilli)
  }

  def eventsDF(spark: SparkSession): Dataset[PlaneData] = {
    import spark.implicits._
    spark.sparkContext.parallelize(eventsStream).toDS()
  }

  def main(args: Array[String]): Unit = {
    val eventsBatches = eventsStream.grouped(100)

    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("client.id", "generator")
    val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)
    val topic = "plane_data"
    try {
      implicit val format: Formats = DefaultFormats
      for (
        batch <- eventsBatches;
        event <- batch
      ) {
        val jsonEvent = write(event)
        producer.send(new ProducerRecord[String, String](topic, jsonEvent)).get()
        println(s"sent $jsonEvent")
      }
    } finally {
      producer.close()
    }
  }
}


case class PlaneData(planeId: Long, x: Double, y: Double, z: Double, timestamp: Long, velocity: Double)

object PlaneData {
  def random(id: Long, timestamp: Long): PlaneData = PlaneData(
    id, Random.nextDouble(), Random.nextDouble(), Random.nextDouble(), timestamp, Random.nextInt(900)
  )
}

case class PlaneDataWithVelocities(
                                    planeId: Long,
                                    x: Double,
                                    y: Double,
                                    z: Double,
                                    timestamp: Long,
                                    velocity: Double,
                                    avgVelocity: Double,
                                    maxVelocity: Double,
                                    minVelocity: Double
                                  )

case class PlaneDataWithNearestPlanes(
                                       planeId: Long,
                                       x: Double,
                                       y: Double,
                                       z: Double,
                                       timestamp: Long,
                                       velocity: Double,
                                       avgVelocity: Double,
                                       maxVelocity: Double,
                                       minVelocity: Double,
                                       nearestPlanes: Set[(Double, Long)]
                                     )

case class EnrichedPlaneData(
                              planeId: Long,
                              x: Double,
                              y: Double,
                              z: Double,
                              timestamp: Long,
                              velocity: Double,
                              avgVelocity: Double,
                              maxVelocity: Double,
                              minVelocity: Double,
                              nearestPlanes: Set[(Double, Long)],
                              adress: Option[String]
                            )