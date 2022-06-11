ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "projectWeatherForecast"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.2.1"
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % "8.1.0"
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-core" % "8.1.0"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "8.2.2"
