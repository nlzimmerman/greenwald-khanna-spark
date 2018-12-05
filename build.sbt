import Dependencies._

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

// addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

// spName := "greenwald-khanna-udaf"
//
// sparkVersion := "2.4.0"
//
// sparkComponents ++= Seq("mllib", "sql")

val sparkVersion = "2.4.0"

// https://github.com/phatak-dev/spark2.0-examples/blob/master/build.sbt
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  // "org.apache.spark" %% "spark-streaming" % sparkVersion,
  // "org.apache.spark" %% "spark-hive" % sparkVersion,
)

resolvers += Resolver.mavenLocal

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT",
    )),
    name := "greenwald-khanna udaf",
    libraryDependencies += scalaTest % Test
  )
