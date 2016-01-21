name := "sparkML"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
  "com.typesafe.slick" %% "slick" % "1.0.0",
  "org.xerial" % "sqlite-jdbc" % "3.7.2"
)