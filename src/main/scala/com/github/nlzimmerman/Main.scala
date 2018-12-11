package com.github.nlzimmerman
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import scala.util.Random

case class DemoKeyVal(val key: String, val n: Double) extends Serializable

object Demo {
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd.RDD._
  import org.apache.spark.rdd.PairRDDFunctions
  // oof
  // https://stackoverflow.com/questions/16921168/scala-generic-method-no-classtag-available-for-t
  import scala.reflect.ClassTag
  def getQuantile[T: ClassTag](
    r: RDD[(T, Double)],
    quantiles: Seq[Double],
    epsilon: Double = 0.01
  ): RDD[(T, (Double, Double))] = {
    val p: PairRDDFunctions[T, Double] = new PairRDDFunctions[T, Double](r)

    val aggregated: PairRDDFunctions[T, GKRecord] = p.aggregateByKey[GKRecord](
      new GKRecord(epsilon)
    )(
      (g: GKRecord, v: Double) => {
        g.insert(v)
      }: GKRecord,
      (a: GKRecord, b: GKRecord) => { a.combine(b) }: GKRecord
    )
    aggregated.flatMapValues(
      (a: GKRecord) => {
        quantiles.map(
          (q: Double) => (q, a.query(q))
        )
      }: Seq[(Double, Double)]
    )
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
  val numbers2: Seq[Double] = (0 until 1000000).map(
    (x) => {
      val u: Double = r.nextDouble()
      val v: Double = r.nextDouble()
      math.pow(-2 * math.log(u), 0.5) * math.cos(2*math.Pi*v)
    }
  )
  println(DirectQuantile.quantile(numbers, Seq(0.05, 0.5, 0.95)))
  println(DirectQuantile.quantile(numbers2, Seq(0.05, 0.5, 0.95)))
  val d: GKRecord = new GKRecord(0.01)
  numbers.foreach(
    (x) => d.insert(x)
  )
  println(Seq(0.05, 0.5, 0.95).map((x) => d.query(x)))
  val n0: RDD[(String, Double)] = spark.
    sparkContext.
    parallelize(numbers).
    map((x) => ("a", x))
  val n1: RDD[(String, Double)] = spark.
    sparkContext.
    parallelize(numbers2).
    map((x) => ("b", x))
  val nr: RDD[(String, Double)] = n0.union(n1).repartition(100)
  println(Demo.getQuantile[String](nr, Seq(0.05, 0.5, 0.95)).collect.toSeq)
  //val g: GKRecord = nr.aggregate(new GKRecord(0.01))((a: GKRecord, b: Double) => a.insert(b), (a: GKRecord, b: GKRecord) => a.combine(b))
  //val g0: GKRecord = nr.treeAggregate(new GKRecord(0.01))((a: GKRecord, b: Double) => a.insert(b), (a: GKRecord, b: GKRecord) => a.combine(b))
  //println(Seq(0.05, 0.5, 0.95).map((x) => g0.query(x)))
  //d.sample.foreach(println(_))
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}
