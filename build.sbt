name := "SECON2019"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= {
  val sparkV = "2.4.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkV,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
    "com.softwaremill.sttp" %% "core" % "1.5.12",
    "com.typesafe.akka" %% "akka-stream" % "2.5.21",
    "com.typesafe.akka" %% "akka-http" % "10.1.7",
    "ch.qos.logback" % "logback-classic" % "1.1.3",
  )
}