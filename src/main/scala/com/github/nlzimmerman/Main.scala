package com.github.nlzimmerman
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
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
  def addArray(x: Seq[Double]): Double = x.reduce(_ + _)
}

object Main extends App {
  import org.apache.spark.SparkContext._
  import com.github.nlzimmerman._
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  lazy val spark: SparkSession = {
    SparkSession.
    builder().
    master("local[4]").
    appName("example").
    getOrCreate()
  }
  import spark.sqlContext.implicits._
  // val b: Seq[Int] = Seq(1,2,3,2)
  // val r: GKRecord[Int] = b.foldLeft(new GKRecord[Int](0.1))((x: GKRecord[Int], a: Int) => x.insert(a))
  // println(r.sample)
  // println(r.compress.sample)
  case class Entry(name: String, value: Int)
  case class DoubleEntry(name: String, value: Double)
  val n0: Dataset[Entry] = Seq(
    Entry("a", 0),
    Entry("a", 1),
    Entry("a", 2),
    Entry("a", 3),
    Entry("a", 4),
    Entry("b", 5),
    Entry("b", 6),
    Entry("b", 7),
    Entry("b", 8),
    Entry("b", 9)
  ).toDS
  n0.show
  val n0double: Dataset[DoubleEntry] = n0.map(
    (x: Entry) => DoubleEntry(x.name, x.value.toDouble)
  )
  n0double.show
  //val n1: Dataset[Double] = n0.map(_.value)
  // println(n1.show)
  val medianizer: GKAggregator[Int] = new GKAggregator[Int](Seq(0.5), 0.01)
  val medianizerDouble: GKAggregator[Double] = new GKAggregator[Double](Seq(0.5), 0.01)
  // val result = n1.select(medianizer.toColumn)

  n0double.groupByKey(_.name).mapValues(_.value).
    agg(medianizerDouble.toColumn.name("median")).show
  println(
    n0double.groupByKey(_.name).mapValues(_.value).
      agg(medianizerDouble.toColumn.name("median")).collect.mkString("\n")
  )

  val n2: DataFrame = n0.toDF
  n2.show
  val medianizerUntyped: UntypedGKAggregator = new UntypedGKAggregator(Seq(0.5), 0.01)
  println("DS untyped")
  n0.groupBy($"name").agg(medianizerUntyped($"value").alias("median")).show
  println("DF untyped")
  n0.toDF.groupBy($"name").agg(medianizerUntyped($"value").alias("median")).show
  println(
    n0.groupBy($"name").agg(medianizerUntyped($"value").alias("median")).
      collect.mkString("\n")
  )

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
