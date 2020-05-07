name := "data_lego"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

// By default, the dist task will include the API documentation in the generated package.
// This is not necessary for us so add the following 2 lines to disable that.
sources in (Compile,doc) := Seq.empty
publishArtifact in (Compile, packageDoc) := false

libraryDependencies ++= Seq(
  "org.mongodb" %% "casbah" % "3.1.1",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.4.0",
  "org.apache.kafka" %% "kafka" % "0.9.0.1",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2",
  "org.apache.hive" % "hive-jdbc" % "1.2.1",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "org.quartz-scheduler" % "quartz" % "2.2.1",
  ws
)

