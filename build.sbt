name := "sparkML"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "org.apache.commons" % "commons-dbcp2" % "2.0.1",
  "org.xerial" % "sqlite-jdbc" % "3.8.11.1",
  "com.rockymadden.stringmetric" % "stringmetric-core_2.10" % "0.27.2",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.2"
)

parallelExecution in Test := false

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"


