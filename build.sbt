resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

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
      organization := "org.github.nlzimmerman",
      scalaVersion := "2.11.12",
      version      := "0.0.1",
    )),
    name := "greenwald-khanna udaf",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
