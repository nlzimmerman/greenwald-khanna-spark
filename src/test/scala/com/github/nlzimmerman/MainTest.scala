package com.github.nlzimmerman

import org.scalatest.{WordSpec, Ignore}
import java.util.ArrayList
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}

object TestParams {
  val targets: Seq[Double] = Seq(
    0.08,
    0.1,
    0.5,
    0.77,
    0.91
  )
}

class Counter[T](val x: Seq[T])(implicit num: Numeric[T]) {
  import num._
  def count: Long = x.length.toLong
  def sum: T = x.reduce(_ + _)
}

object Util {
  import org.apache.spark.sql.SparkSession
  lazy val spark: SparkSession = {
    SparkSession.
    builder().
    master("local[4]").
    appName("example").
    getOrCreate()
  }
  import org.apache.commons.math3.special.Erf
  def inverseNormalCDF(q: Double): Double =
    math.sqrt(2)*Erf.erfInv(2*q-1)
  def normalCDF(x: Double): Double =
    0.5*(1+Erf.erf(x/math.sqrt(2)))
  def inverseNormalCDFBounds(quantiles: Seq[Double], epsilon: Double): Seq[(Double, Double)] = {
    quantiles.map(
      (q) => (inverseNormalCDF(q-epsilon), inverseNormalCDF(q+epsilon))
    )
  }
  def directQuantileBounds(
    quantiles: Seq[Double],
    epsilon: Double,
    seq: Seq[Double]
  ): Seq[(Double, Double)] = {
    val quantilesWithEpsilon: Seq[Double] = quantiles.flatMap(
      (x) => Seq(x-epsilon, x+epsilon)
    )
    val lookup: Seq[Double] = DirectQuantile.getQuantiles(seq, quantilesWithEpsilon)
    (0 until lookup.length by 2).map(
      (i) => (lookup(i), lookup(i+1))
    )
  }
  def boundsCheck[T](n: Seq[T], b: Seq[(T, T)])
      (implicit num: Numeric[T]): Unit = {
    import num._
    assert(n.length == b.length)
    n.zip(b).foreach(
      (x: (T, (T, T))) => assert(
        (x._2._1 <= x._1) &&
        (x._1 <= x._2._2)
      )
    )
  }
}

object NormalNumbers {
  import scala.util.Random
  private val r: Random = new Random(2210)
  def nextNormal(): Double = {
    val u: Double = r.nextDouble()
    val v: Double = r.nextDouble()
    math.pow(-2 * math.log(u), 0.5) * math.cos(2*math.Pi*v)
  }
  // this is the EXACT normal distribution, to make sure that DirectQuantile works
  val exactNumbers: Seq[Double] = r.shuffle((1 until 500000).toList).map(
    (x) => Util.inverseNormalCDF(x.toDouble/500000)
  )
  // this is a random sampling of the normal distribution.
  val numbers: Seq[Double] = (0 until 500000).map((x) => nextNormal())
  val numbers2: Seq[Double] = (0 until 500000).map((x) => nextNormal())
}


class MainSuite extends WordSpec {

  "DirectQuantile" should {
    "behave reasonably for a small list of ints" in {
      import scala.util.Random
      /*  this is just the 100 numbers from 0 to 99, inclusive.
          so, I'm reasoning, the 1st percentile should be 0,
          the 100th percentile should be 99,
          the 50th percentile should be 49.
          right?
      */
      val n: Seq[Int] = (0 until 100)/*.map(_.toDouble)*/.toList
      val rand: Random = new Random(2200)
      val nShuffle: Seq[Int] = rand.shuffle(n)
      DirectQuantile.getQuantiles(n, Seq(0.1, 0.15, 0.61, 0.99)).zip(
        Seq[Int](9, 14, 60, 98)
      ).foreach({
        case(a: Int, b: Int) => assert(a==b)
      })
    }
    "be able to invert the exact normal distribution" in {
      import Util._
      import NormalNumbers._
      import TestParams._
      val bounds: Seq[(Double, Double)] = inverseNormalCDFBounds(targets, 2.toDouble/500000)
      val m: Seq[Double] = DirectQuantile.getQuantiles(exactNumbers, targets)
      // val n: Seq[Double] = DirectQuantile.getQuantiles(numbers2, targets)
      boundsCheck(m, bounds)
      // boundsCheck(n, bounds)
    }
  }
  "GKQuantile" should {
    "behave reasonably for a very small list of Ints" in {
      // this is the bug we need to not fall victim to
      // https://www.stevenengelhardt.com/2018/03/07/calculating-percentiles-on-streaming-data-part-2-notes-on-implementing-greenwald-khanna/#GK01
      val b: Seq[Int] = Seq(11,20,18,5,12,6,3,2)
      val r: GKRecord[Int] = b.foldLeft(new GKRecord[Int](0.1))((x: GKRecord[Int], a: Int) => x.insert(a))
      // val r2: GKRecord = b.foldLeft(new GKRecord(0.01))((x: GKRecord, a: Double) => x.insert(a))
      // needs to return something with rank between 0.4*8=3.2 and 0.6*8=4.8
      // 4 is the only integer in that range so 6.0 is the only thing that can match.
      assert(r.query(0.5)==6)
      assert(r.query(0.00001)==2)
      // needs to return something with rank between 6.4 and 8.0
      assert(r.query(0.9)==18 || r.query(0.9)==20)
    }
    "be able to combine equal values" when {
      "they are in the middle of the list" in {
        val b: Seq[Int] = Seq(1,2,3,2)
        val r: GKRecord[Int] = b.foldLeft(new GKRecord[Int](0.1))((x: GKRecord[Int], a: Int) => x.insert(a))
        assert(r.sample==Seq(
          GKEntry(1,1,0),
          GKEntry(2,1,0),
          GKEntry(2,1,0),
          GKEntry(3,1,0)
        ))
        assert(r.compress.sample==Seq(
          GKEntry(1,1,0),
          GKEntry(2,1,1),
          GKEntry(3,2,0)
        ))
      }
      "they are at the beginning of the list" in {
        val b: Seq[Int] = Seq(1,2,3,1)
        val r: GKRecord[Int] = b.foldLeft(new GKRecord[Int](0.1))((x: GKRecord[Int], a: Int) => x.insert(a))
        assert(r.sample==Seq(
          GKEntry(1,1,0),
          GKEntry(1,1,0),
          GKEntry(2,1,0),
          GKEntry(3,1,0)
        ))
        assert(r.compress.sample==Seq(
          GKEntry(1,1,1),
          GKEntry(2,2,0),
          GKEntry(3,1,0)
        ))
      }
      "they are at the end of the list" in {
        val b: Seq[Int] = Seq(3,1,2,3)
        val r: GKRecord[Int] = b.foldLeft(new GKRecord[Int](0.1))(
          (x: GKRecord[Int], a: Int) => {
            x.insert(a)
          }
        )
        assert(r.sample==Seq(
          GKEntry(1,1,0),
          GKEntry(2,1,0),
          GKEntry(3,1,0),
          GKEntry(3,1,0)
        ))
        assert(r.compress.sample==Seq(
          GKEntry(1,1,0),
          GKEntry(2,1,0),
          GKEntry(3,1,1)
        ))
      }
    }
    "behave reasonably for a fairly small list of Ints" in {
      import scala.util.Random
      val n: Seq[Int] = (0 until 100).toList
      val rand: Random = new Random(2200)
      val nShuffle: Seq[Int] = rand.shuffle(n)
      val q: Seq[Int] = GKQuantile.getQuantiles(nShuffle, Seq(0.0, 0.1, 0.15, 0.61, 0.99, 1.0), 0.05)

      val bounds: Seq[(Int, Int)] = Seq(
        (-1, 5), (5, 15), (10, 20), (56, 66), (94, 101), (94, 101)
      )
      Util.boundsCheck(q, bounds)
    }

    "be able to invert the normal distribution" when {
      import Util._
      import NormalNumbers._
      import TestParams._
      // this checks against the actual distribution
      // using DirectQuantile. It is SLOW.
      def gkCheck(epsilon: Double): Unit = {
        val bounds: Seq[(Double, Double)] = directQuantileBounds(targets, epsilon, numbers)
        //val bounds2: Seq[(Double, Double)] = directQuantileBounds(targets, epsilon, numbers2)
        val n: Seq[Double] = GKQuantile.getQuantiles(numbers, targets, epsilon)
        //val m: Seq[Double] = GKQuantile.getQuantiles(numbers2, targets, epsilon)
        boundsCheck(n, bounds)
        //boundsCheck(m, bounds2)
      }
      // this checks against what the bounds should be, assuming that the
      // normal distribution is perfectly sampled. It works fine except at very small epsilon.
      def gkCheckFast(epsilon: Double): Unit = {
        val bounds: Seq[(Double, Double)] = inverseNormalCDFBounds(targets, epsilon)
        val n: Seq[Double] = GKQuantile.getQuantiles(numbers, targets, epsilon)
        boundsCheck(n, bounds)
      }
      "epsilon = 0.001" in {
        gkCheck(0.001)
      }
      "epsilon = 0.005" in {
        gkCheckFast(0.005)
      }
      "epsilon = 0.01" in {
        gkCheckFast(0.01)
      }
      "epsilon = 0.05" in {
        gkCheckFast(0.05)
      }
    }
  }
}

class SparkSuite extends WordSpec {
  "GKQuantile" should {
    "be able to invert the normal distribution in Spark" when {
      import org.apache.log4j.{Level, Logger}
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
      import Util._
      import NormalNumbers._
      import TestParams._

      val n0: RDD[Double] = spark.
        sparkContext.
        parallelize(numbers)
      def gkCheckFast(epsilon: Double): Unit = {
        val bounds: Seq[(Double, Double)] = inverseNormalCDFBounds(targets, epsilon)
        val n: Seq[Double] = GKQuantile.getQuantiles(numbers, targets, epsilon)
        boundsCheck(n, bounds)
      }
      "epsilon = 0.005" in {
        gkCheckFast(0.005)
      }
      "epsilon = 0.01" in {
        gkCheckFast(0.01)
      }
      "epsilon = 0.05" in {
        gkCheckFast(0.05)
      }


    }
    "be able to invert the normal distribution in Spark by key" when {
      import org.apache.log4j.{Level, Logger}
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
      import Util._
      import NormalNumbers._
      import TestParams._
      import org.apache.spark.rdd.{RDD, PairRDDFunctions}
      val n0: RDD[(String, Double)] = spark.
        sparkContext.
        parallelize(numbers).
        map((x) => ("a", x))
      val n1: RDD[(String, Double)] = spark.
        sparkContext.
        parallelize(numbers2).
        map((x) => ("b", x))
      val nr: RDD[(String, Double)] = n0.union(n1).repartition(100)
      def gkCheckSpark(epsilon: Double): Unit = {
        val bounds: Seq[(Double, Double)] = inverseNormalCDFBounds(targets, epsilon)
        // this is a hack to make sure that getGroupedQuantilesDouble works. I need to spin it out into its own test soon!!
        val quantiles: Map[(String, Double), Double] =
          {
            {
              GKQuantile.getGroupedQuantiles(nr, targets, epsilon)
            }: RDD[((String, Double), Double)]
          }.collectAsMap.toMap
        val aValues: Seq[Double] = targets.map((x: Double) => quantiles(("a", x)))
        val bValues: Seq[Double] = targets.map((x: Double) => quantiles(("b", x)))
        boundsCheck(aValues, bounds)
        boundsCheck(bValues, bounds)
      }
      "epsilon = 0.005" in {
        gkCheckSpark(0.005)
      }
      "epsilon = 0.01" in {
        gkCheckSpark(0.01)
      }
      "epsilon = 0.05" in {
        gkCheckSpark(0.05)
      }
    }
  }
}

/** case class has to be defined outside of the class where toDS will be
  * called or else the implicits won't work. IDGI
  */

case class LabeledNumber(name: String, value: Double)
case class LabeledQuantile(name: String, quantile: Map[Double, Double])

class SQLSuite extends WordSpec {
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  import Util._
  import NormalNumbers._
  import TestParams._
  import org.apache.spark.SparkContext._
  import spark.sqlContext.implicits._
  "GKAggregator" should {

    "be able to invert the normal distribution in Spark by Key" when {
      val ds: Dataset[LabeledNumber] = {
        numbers.map((x: Double) => LabeledNumber("a", x)).toDS.union(
          numbers2.map((x: Double) => LabeledNumber("b", x)).toDS()
        ).repartition(100)
      }
      def checker(epsilon: Double): Unit = {
        val bounds: Seq[(Double, Double)] = inverseNormalCDFBounds(targets, epsilon)
        val quantiles: Seq[LabeledQuantile] = {
          val quantilizer: GKAggregator[Double] = new GKAggregator[Double](targets, 0.01)
          val qTuple: Dataset[(String, Map[Double, Double])] = ds.
              groupByKey(_.name).mapValues(_.value).
              agg(quantilizer.toColumn.name("quantile"))
          val q: Dataset[LabeledQuantile] = qTuple.map(LabeledQuantile.tupled(_))
          q.collect
        }
        val keyedQuantiles: Map[String, Map[Double, Double]] = quantiles.map(
          (x: LabeledQuantile) => x.name -> x.quantile
        ).toMap
        val aValues: Seq[Double] = targets.map(
          (x: Double) => keyedQuantiles("a")(x)
        )
        val bValues: Seq[Double] = targets.map(
          (x: Double) => keyedQuantiles("b")(x)
        )
        boundsCheck(aValues, bounds)
        boundsCheck(bValues, bounds)
      }
      "epsilon = 0.005" in {
        checker(0.005)
      }
      "epsilon = 0.01" in {
        checker(0.01)
      }
      "epsilon = 0.05" in {
        checker(0.05)
      }
    }
  }

}
