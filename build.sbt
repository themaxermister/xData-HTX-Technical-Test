ThisBuild / version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .enablePlugins(JacocoItPlugin)
  .settings(
    name := "htx_scala_spark"
  )

val sparkVersion = "3.5.1"
val scalaCheckVersion = "1.17.0"
val scalaLoggingVersion = "3.9.5"
val logBackVersion = "1.5.3"
val junitJupiterVersion = "5.10.2"
val jUnitInterfaceVersion = "0.11.1"
val mockitoVersion = "5.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "ch.qos.logback" % "logback-classic" % logBackVersion % Test,
  "org.junit.jupiter" % "junit-jupiter-api" % junitJupiterVersion % Test,
  "org.junit.jupiter" % "junit-jupiter-params" % junitJupiterVersion % Test,
  "org.junit.jupiter" % "junit-jupiter-engine" % junitJupiterVersion % Test,
  "org.mockito" % "mockito-core" % mockitoVersion % Test,
  "org.mockito" % "mockito-junit-jupiter" % mockitoVersion % Test,
  "net.aichler" % "jupiter-interface" % jUnitInterfaceVersion % Test
)
