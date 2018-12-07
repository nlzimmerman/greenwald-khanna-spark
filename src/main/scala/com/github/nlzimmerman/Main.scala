package com.github.nlzimmerman
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import scala.util.Random

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
  val r: Random = new Random(2210)
  val numbers: Seq[Double] = (0 until 100000).map(
    (x) => {
      val u: Double = r.nextDouble()
      val v: Double = r.nextDouble()
      math.pow(-2 * math.log(u), 0.5) * math.cos(2*math.Pi*v)
    }
  )
  println(DirectQuantile.quantile(numbers, Seq(0.05, 0.5, 0.95)))
  val d: GKRecord = new GKRecord(0.01)
  numbers.foreach(
    (x) => d.insert(x)
  )
  println(Seq(0.05, 0.5, 0.95).map((x) => d.query(x)))
  //d.sample.foreach(println(_))
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}
