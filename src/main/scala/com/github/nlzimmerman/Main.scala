package com.github.nlzimmerman
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import scala.util.Random

case class DemoKeyVal(val key: String, val n: Double) extends Serializable

object Main extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  lazy val spark: SparkSession = {
    SparkSession.
    builder().
    master("local[4]").
    appName("example").
    getOrCreate()
  }
  //
  // println(spark.sparkContext.parallelize(Seq(1,2,3)).reduce(_ + _))
  /*  Spark tends to throw errors in local mode when shutting down;
      let's just suppress them.
   */
  // val n0: RDD[(String, Double)] = spark.
  //   sparkContext.
  //   parallelize(numbers).
  //   map((x) => ("a", x))
  // val n1: RDD[(String, Double)] = spark.
  //   sparkContext.
  //   parallelize(numbers2).
  //   map((x) => ("b", x))
  // val nr: RDD[(String, Double)] = n0.union(n1).repartition(100)
  // println(GKQuantile.getGroupedQuantiles(nr, Seq(0.05, 0.5, 0.95)).collect.toSeq)
  //val g: GKRecord = nr.aggregate(new GKRecord(0.01))((a: GKRecord, b: Double) => a.insert(b), (a: GKRecord, b: GKRecord) => a.combine(b))
  //val g0: GKRecord = nr.treeAggregate(new GKRecord(0.01))((a: GKRecord, b: Double) => a.insert(b), (a: GKRecord, b: GKRecord) => a.combine(b))
  //println(Seq(0.05, 0.5, 0.95).map((x) => g0.query(x)))
  //d.sample.foreach(println(_))
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}
