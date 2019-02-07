package com.github.nlzimmerman

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.api.java.JavaRDD
import java.util.{
  ArrayList,
  Map => JavaMap
}
import scala.collection.JavaConversions._

/** This package includes convenience functions that might be useful to a person
  * writing in Scala and are definitely necessary to make the Pyspark wrapper work.
  */
object GKQuantile {
  // oof
  // https://stackoverflow.com/questions/16921168/scala-generic-method-no-classtag-available-for-t
  import scala.reflect.ClassTag

  // these mostly exist so that the python functions of the same name
  // can call them, because the
  // objects would be a huge pain to directly manipulat in Pyspark.
  // this takes a sequence, not an RDD. It seems unlikely that it would ever be called.
  def getQuantiles[T](
    x: Seq[T],
    quantiles: Seq[Double],
    epsilon: Double=0.01
  )(implicit num: Numeric[T]): Map[Double, T] = {
    import num._
    val d: GKRecord[T] = x.foldLeft(new GKRecord[T](epsilon))(
      (x: GKRecord[T], y: T) => x.insert(y)
    )
    quantiles.map((q: Double) => (q -> d.query(q))).toMap
  }
  // here's the one that will get called in Spark.
  def getQuantiles[T](
    x: RDD[T],
    quantiles: Seq[Double],
    epsilon: Double
  )(implicit num: Numeric[T]): Map[Double, T] = {
    import num._
    val d: GKRecord[T] = x.treeAggregate(
      new GKRecord[T](epsilon)
    )(
      (a: GKRecord[T], b: T) => a.insert(b),
      (a: GKRecord[T], b: GKRecord[T]) => a.combine(b)
    )
    quantiles.map((q: Double) => (q -> d.query(q))).toMap
  }

  /** The python function getQuantiles calls these two functions
    * Since py4j can't understand generics (since it's working with compiled/
    * type-erased code), it does type inspections and
    * then calls these typed functions.
    */
  def _PyGetQuantilesInt(
    x: JavaRDD[Int],
    quantiles: ArrayList[Double],
    epsilon: Double
    // remember, this works because of an implicit conversion.
  ): JavaMap[Double, Int] = getQuantiles(x, quantiles.toSeq, epsilon)

  def _PyGetQuantilesDouble(
    x: JavaRDD[Double],
    quantiles: ArrayList[Double],
    epsilon: Double
  ): JavaMap[Double, Double] = getQuantiles(x, quantiles.toSeq, epsilon)

  /** I changed the output of this function as I was getting ready for 1.0
    * Formerly, it returned an RDD[((U, Double), T)], i.e.
    * ((key, quantile), value)
    * since (quantile, value) tuples should be very small, I'm moving this over
    * to being a map.
    */
  def getGroupedQuantiles[U:ClassTag, T: ClassTag](
    r: RDD[(U, T)],
    quantiles: Seq[Double],
    epsilon: Double = 0.01
  )(implicit num: Numeric[T]): RDD[(U, Map[Double, T])] = {
    import num._
    // this makes conversion to and from PairRDDFunctions automatic
    import org.apache.spark.SparkContext._
    //val p: PairRDDFunctions[U, T] = new PairRDDFunctions[U, T](r)
    val p: PairRDDFunctions[U, T] = r
    val aggregated: PairRDDFunctions[U, GKRecord[T]] = p.aggregateByKey[GKRecord[T]](
      new GKRecord[T](epsilon)
    )(
      (g: GKRecord[T], v: T) => {
        g.insert(v)
      }: GKRecord[T],
      (a: GKRecord[T], b: GKRecord[T]) => { a.combine(b) }: GKRecord[T]
    )
    aggregated.mapValues(
      (a: GKRecord[T]) => {
        quantiles.map(
          (q: Double) => (q -> a.query(q))
        ).toMap
      }
    )
  }


  /* Python compatibility to the above */
  /*  this is called by _PyGetGroupedQuantilesDouble and _PyGetGroupedQuantilesInt
    * it's private just so I don't forget what it's here for. :)
    */
  private def pyToTuple2[T](x: JavaRDD[Any]): RDD[(Any, T)] = {
    /*
    * this takes advantage of the implicits imported at the top of the file.
    * there may be a more parsimonious way of writing this but I don't think this
    * way is slower.
    */
    // JavaRDD[Any] to RDD[Any]
    val asRDDAny: RDD[Any] = x
    val asRDDArray: RDD[Array[Any]] = asRDDAny.map(
      (x: Any) => x.asInstanceOf[Array[Any]]
    )
    val asRDDTuple: RDD[(Any, T)] = asRDDArray.map(
      (x: Array[Any]) => {
        // is this check really necessary? I don't think it slows things down much.
        if (x.length != 2) throw new Exception(s"Array $x is not of length 2.")
        (x(0), x(1).asInstanceOf[T])
      }
    )
    asRDDTuple
  }
  // this gets the output of getGroupedQuantiles back into Python tuples, which py4j
  // understands as Arrays
  private def groupedQuantilesToPython[T](x: RDD[(Any, Map[Double,T])]): JavaRDD[Array[Any]] = {
    val calculatedArrays: RDD[Array[Any]] = x.map(
      (y: (Any, Map[Double, T])) => {
        // this leverages the JavaConverters import
        val y2: JavaMap[Double, T] = y._2
        Array(y._1, y2)
      }
    )
    /*  this also takes advantage of one of the implicits we imported at the top
      * of the file.
      */
    calculatedArrays.toJavaRDD
  }
  def _PyGetGroupedQuantilesDouble(
    r: JavaRDD[Any], // needs to be a Python RDD of (string, float)
                     // we will do type coercion to make this the case
                     // and that will give a runtime error if that's not the case.
    quantiles: ArrayList[Double], // this is a Python list of float
    epsilon: Double = 0.01        // just a python float
  ): JavaRDD[Array[Any]] = { // this is ((String, Double), Double) but with both tuples converted to Arrays.
    /*
    * this takes advantage of the implicits imported at the top of the file.
    * there may be a more parsimonious way of writing this but I don't think this
    * way is slower.
    *
    * Also, this might be a little bit dangerous: I'm not inspecting the type of the key
    * at ALL: it stays Any all the way through. I haven't read up on whether this is
    * safe with reduceByKey or not.
    */
    // JavaRDD[Any] to RDD[Any]
    val rScala: RDD[(Any, Double)] = pyToTuple2(r)
    // now we have the types straight so we can actually do the grouped quantiles.
    val calculated: RDD[(Any, Map[Double,Double])] = getGroupedQuantiles(
      rScala, quantiles, epsilon
    )
    // and now we need to get that back into something that py4j can handle.
    // which, again, is nested Arrays.
    groupedQuantilesToPython(calculated)
  }
  def _PyGetGroupedQuantilesInt(
    r: JavaRDD[Any],
    quantiles: ArrayList[Double],
    epsilon: Double = 0.01
  ): JavaRDD[Array[Any]] = {
    val rScala: RDD[(Any, Int)] = pyToTuple2(r)
    val calculated: RDD[(Any, Map[Double,Int])]= getGroupedQuantiles(
      rScala, quantiles, epsilon
    )
    groupedQuantilesToPython(calculated)
  }

}
