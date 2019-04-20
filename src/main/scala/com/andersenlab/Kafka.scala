package com.andersenlab

import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

object Kafka {
  def read(spark: SparkSession)(bootstrapServers: String, topic: String, schema: StructType): DataFrame = {
    import spark.implicits._
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .load()
      .select(from_json($"value" cast StringType, schema) as "json")
      .select($"json.*")
      .withColumn("timestamp", $"timestamp" cast TimestampType)
  }

  import org.apache.spark.sql.functions.{to_json, col, struct}

  def write(dataset: Dataset[_])(bootstrapServers: String, topic: String, checkpoint: String): StreamingQuery =
    dataset
      .select(to_json(struct(col("*"))) as "value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .option("checkpointLocation", checkpoint)
      .start()

}

object Console {
  def write(dataset: Dataset[_]): StreamingQuery =
    dataset
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start()
}