package com.andersenlab

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization.read
import com.softwaremill.sttp._
import scala.util.Try


object AdressEnricherAttempt2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("adress_enricher")
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val eventsStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("plane_data_with_addresses"), kafkaParams)
    )

    eventsStream.transform { batch =>
      val offsetRanges = batch.asInstanceOf[HasOffsetRanges].offsetRanges
      val mapped = batch.mapPartitions { eventsPartition =>

        implicit val formats: Formats = DefaultFormats
        implicit val backend: SttpBackend[Try, Nothing] = TryHttpURLConnectionBackend()
        val parsed = eventsPartition.map { record =>
          read[PlaneDataWithNearestPlanes](record.value())
        }

        def getAddress(latitude: Double, longitude: Double): Option[String] = {
          val request = sttp.get(uri"""http://localhost:8080/geocode?latitude=$latitude&longitude=$longitude""")

          val response = request.send()

          val address: Option[String] = response.toOption.flatMap(_.body.fold(error => None, Some(_)))

          address
        }

        val withAddresses = parsed.map { event =>
          EnrichedPlaneData(event.planeId, event.x, event.y, event.z, event.timestamp, event.velocity, event.avgVelocity, event.maxVelocity, event.minVelocity, event.nearestPlanes,
            getAddress(event.x, event.y))
        }

        withAddresses
      }
      eventsStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      mapped
    }.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
