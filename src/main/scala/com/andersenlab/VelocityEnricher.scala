package com.andersenlab

import java.time.Instant
import java.util.Locale
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.TimestampType
import scala.util.Random

object VelocityEnricher {
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

    val planeData: DataFrame =
      Kafka.read(spark)(bootstrapServers = "kafka:9092", topic = "plane_data", schema = Schemas.PlaneEvent)

    val windowSpec = Window
      .partitionBy(
        window($"timestamp", windowDuration = "1 minutes", slideDuration = "1 minutes"),
        $"plane_id"
      )

    val dataWithVelocities = {
      //      planeData
      //        .withColumn("avgVelocity", avg($"velocity") over windowSpec)
      //        .withColumn("maxVelocity", max($"velocity") over windowSpec)
      //        .withColumn("minVelocity", min($"velocity") over windowSpec)

      planeData
        .as[PlaneData]
        .groupByKey(event => (event.timestamp / 1000) -> event.planeId)
        .flatMapGroups { case (_, events) =>
          val eventsVector = events.toVector
          val velocities = eventsVector.map(_.velocity)

          val minVelocity = velocities.min
          val maxVelocity: Double = velocities.max
          val avgVelocity = velocities.sum / velocities.size

          eventsVector.map { event =>
            PlaneDataWithVelocities(event.planeId, event.x, event.y, event.z, event.timestamp, event.velocity, avgVelocity, maxVelocity, minVelocity)
          }
        }
    }


    Kafka.write(dataWithVelocities)(
      bootstrapServers = "kafka:9092", topic = "plane_data_with_velocities", checkpoint = "./checkpoint-velocities"
    ).awaitTermination()

  }
}