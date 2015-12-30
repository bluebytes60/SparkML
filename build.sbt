name := "sparkExcerise"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"
)
    