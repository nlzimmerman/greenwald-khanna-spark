package com.github.nlzimmerman
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}


object Main extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  // val spark: SparkSession = {
  //   SparkSession.
  //   builder().
  //   master("local").
  //   appName("example").
  //   getOrCreate()
  // }
  //
  // println(spark.sparkContext.parallelize(Seq(1,2,3)).reduce(_ + _))
  /*  Spark tends to throw errors in local mode when shutting down;
      let's just suppress them.
   */
  println(DirectQuantile.quantile((0 until 1000).map(_.toDouble).toSeq, Seq(0.01, 0.5)))
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}
