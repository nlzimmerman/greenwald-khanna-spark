package com.github.nlzimmerman
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD}
import org.apache.log4j.{Level, Logger}
import scala.util.Random

case class DemoKeyVal(val key: String, val n: Double) extends Serializable

object Python {
  def printHello(): Unit = println("Hello World")
  def returnHello(): String = "Hello World!"
  def addRDD(x: RDD[Double]): Double = x.reduce(_ + _)
  //def addRDD(x: JavaDoubleRDD): Double = x.reduce(_ + _)
  // def addRDD(x: JavaRDD[Double]): Double = x.reduce(
  //   (a: Double, b: Double) => a+b
  // )
  //def add(x: Double, y: Double): Double = x + y
  // loooota boilerplate around numeric types because numeric widening doesn't happen
  // in py2java
  def add(x: Double, y: Double): Double = {
    x + y
  }
  def add(x: Int, y: Int): Int = {
    x + y
  }
  def add(x: Int, y: Double): Double = {
    x.toDouble + y
  }
  def add(x: Double, y: Int): Double = {
    x + y.toDouble
  }
}

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
  val b: Seq[Int] = Seq(1,2,3,2)
  val r: GKRecord[Int] = b.foldLeft(new GKRecord[Int](0.1))((x: GKRecord[Int], a: Int) => x.insert(a))
  println(r.sample)
  println(r.compress.sample)
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
  // val n: Int = 100
  // val numbers: Seq[Double] = (0 until n).flatMap(
  //   (x: Int) => Seq(1,5,2,6,3,7,4,8,0,9).map((z: Int) => (z+10*x).toDouble)
  // ) :+ (n*10).toDouble
  // println(numbers.length)
  // println(DirectQuantile.getQuantiles(numbers, Seq(0.05, 0.5, 0.95)))
  // println(GKQuantile.getQuantiles(numbers, Seq(0.05, 0.5, 0.95)))
  // val gk: GKRecord = numbers.foldLeft(new GKRecord(0.01))((g, i) => g.insert(i))
  // println(gk.sample)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}
