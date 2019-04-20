package com.andersenlab

import org.apache.spark.sql.types._

object Schemas {
  private val baseFields = Seq(
    StructField("planeId", LongType, nullable = false),
    StructField("timestamp", LongType, nullable = false),
    StructField("x", DoubleType, nullable = false),
    StructField("y", DoubleType, nullable = false),
    StructField("z", DoubleType, nullable = false),
    StructField("velocity", DoubleType, nullable = false)
  )

  private val fieldsWithVelocities =
    baseFields ++ Seq(
      StructField("avgVelocity", DoubleType, nullable = false),
      StructField("maxVelocity", DoubleType, nullable = false),
      StructField("minVelocity", DoubleType, nullable = false)
    )

  private val fieldsWithNearestPlains =
    fieldsWithVelocities ++ Seq(
      StructField("nearestPlanes", ArrayType(LongType, containsNull = false), nullable = true)
    )

  private val fieldsWithAddress =
    fieldsWithNearestPlains ++ Seq(
      StructField("address", StringType, nullable = true)
    )

  val PlaneEvent: StructType =
    StructType(
      baseFields
    )

  val PlaneEventWithVelocities: StructType =
    StructType(
      fieldsWithVelocities
    )

  val PlaneEventWithNearestPlains: StructType =
    StructType(fieldsWithNearestPlains)

  val EnrichedPlaneEvent: StructType =
    StructType(fieldsWithAddress)
}
