package com.github.nlzimmerman

import org.scalatest.WordSpec

object TestParams {
  val targets: Seq[Double] = Seq(
    0.08,
    0.1,
    0.5,
    0.77,
    0.91
  )
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
  def boundsCheck(n: Seq[Double], b: Seq[(Double, Double)]): Unit = {
    assert(n.length == b.length)
    n.zip(b).foreach(
      (x: (Double, (Double, Double))) => assert(
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
    "be able to invert the exact normal distribution" in {
      import Util._
      import NormalNumbers._
      import TestParams._
      val bounds: Seq[(Double, Double)] = inverseNormalCDFBounds(targets, 1.toDouble/500000)
      val m: Seq[Double] = DirectQuantile.getQuantiles(exactNumbers, targets)
      // val n: Seq[Double] = DirectQuantile.getQuantiles(numbers2, targets)
      boundsCheck(m, bounds)
      // boundsCheck(n, bounds)
    }
  }
  "GKQuantile" should {
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
      import org.apache.spark.rdd.RDD
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
        val quantiles: Map[(String, Double), Double] =
          GKQuantile.getGroupedQuantiles(nr, targets, epsilon).collectAsMap.toMap
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

class InternalSuite extends WordSpec {
  "GKQuantile" should {
    "be able to specifically recreate the Python internals" when {
      "(WARNING: potential garbage-in garbage-out) n is 1..1000" in {
        // copy-pasted with some regex — gross!
        // to be clear, I copy/pasted d['sample'] from the python and
        // piped it through
        // cat test | sed 's/\[/Seq\(/g' | sed 's/\]/\)/g'
        val result: Seq[Seq[Int]] = Seq(Seq(0, 1, 0), Seq(16, 16, 0), Seq(32, 16, 0), Seq(40, 8, 0), Seq(55, 15, 0), Seq(65, 10, 0), Seq(75, 10, 0), Seq(85, 10, 0), Seq(95, 10, 0), Seq(105, 10, 0), Seq(115, 10, 0), Seq(125, 10, 0), Seq(135, 10, 0), Seq(151, 16, 0), Seq(161, 10, 0), Seq(171, 10, 0), Seq(181, 10, 0), Seq(191, 10, 0), Seq(201, 10, 0), Seq(211, 10, 0), Seq(221, 10, 0), Seq(231, 10, 0), Seq(241, 10, 0), Seq(251, 10, 0), Seq(261, 10, 0), Seq(271, 10, 0), Seq(281, 10, 0), Seq(291, 10, 0), Seq(301, 10, 0), Seq(311, 10, 0), Seq(321, 10, 0), Seq(331, 10, 0), Seq(341, 10, 0), Seq(351, 10, 0), Seq(361, 10, 0), Seq(371, 10, 0), Seq(381, 10, 0), Seq(391, 10, 0), Seq(401, 10, 0), Seq(411, 10, 0), Seq(421, 10, 0), Seq(431, 10, 0), Seq(441, 10, 0), Seq(451, 10, 0), Seq(461, 10, 0), Seq(471, 10, 0), Seq(481, 10, 0), Seq(491, 10, 0), Seq(501, 10, 0), Seq(511, 10, 0), Seq(521, 10, 0), Seq(531, 10, 0), Seq(541, 10, 0), Seq(551, 10, 0), Seq(561, 10, 0), Seq(571, 10, 0), Seq(581, 10, 0), Seq(591, 10, 0), Seq(601, 10, 0), Seq(611, 10, 0), Seq(621, 10, 0), Seq(631, 10, 0), Seq(641, 10, 0), Seq(651, 10, 0), Seq(661, 10, 0), Seq(671, 10, 0), Seq(681, 10, 0), Seq(691, 10, 0), Seq(701, 10, 0), Seq(711, 10, 0), Seq(721, 10, 0), Seq(731, 10, 0), Seq(741, 10, 0), Seq(751, 10, 0), Seq(761, 10, 0), Seq(771, 10, 0), Seq(781, 10, 0), Seq(791, 10, 0), Seq(801, 10, 0), Seq(811, 10, 0), Seq(821, 10, 0), Seq(831, 10, 0), Seq(841, 10, 0), Seq(851, 10, 0), Seq(861, 10, 0), Seq(871, 10, 0), Seq(881, 10, 0), Seq(891, 10, 0), Seq(901, 10, 0), Seq(902, 1, 18), Seq(911, 9, 0), Seq(912, 1, 18), Seq(921, 9, 0), Seq(922, 1, 18), Seq(931, 9, 0), Seq(932, 1, 18), Seq(941, 9, 0), Seq(942, 1, 18), Seq(949, 7, 0), Seq(951, 2, 0), Seq(952, 1, 19), Seq(953, 1, 19), Seq(959, 6, 0), Seq(961, 2, 0), Seq(962, 1, 19), Seq(963, 1, 19), Seq(969, 6, 0), Seq(971, 2, 0), Seq(972, 1, 19), Seq(973, 1, 19), Seq(979, 6, 0), Seq(981, 2, 0), Seq(982, 1, 19), Seq(983, 1, 19), Seq(989, 6, 0), Seq(991, 2, 0), Seq(992, 1, 19), Seq(993, 1, 19), Seq(999, 6, 0), Seq(1000, 1, 0))
        // blah
        println(result.length)
      }
    }
  }
}
