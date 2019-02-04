package com.github.nlzimmerman

// _ imports aren't a great way to learn a language but I use the implicits all the time.
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext._

import scala.annotation.tailrec

object GKQuantile {
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.rdd.PairRDDFunctions
  import org.apache.spark.api.java.JavaRDD
  import java.util.ArrayList
  import scala.collection.JavaConversions._
  // oof
  // https://stackoverflow.com/questions/16921168/scala-generic-method-no-classtag-available-for-t
  import scala.reflect.ClassTag
  def getQuantiles[T](
    x: Seq[T],
    quantiles: Seq[Double],
    epsilon: Double=0.01
  )(implicit num: Numeric[T]): Seq[T] = {
    import num._
    val d: GKRecord[T] = x.foldLeft(new GKRecord[T](epsilon))(
      (x: GKRecord[T], y: T) => x.insert(y)
    )
    quantiles.map((q: Double) => d.query(q))
  }

  def getQuantiles[T](
    x: RDD[T],
    quantiles: Seq[Double],
    epsilon: Double
  )(implicit num: Numeric[T]): Seq[T] = {
    import num._
    val d: GKRecord[T] = x.treeAggregate(
      new GKRecord[T](epsilon)
    )(
      (a: GKRecord[T], b: T) => a.insert(b),
      (a: GKRecord[T], b: GKRecord[T]) => a.combine(b)
    )
    quantiles.map((q: Double) => d.query(q))
  }

  /** The python function getQuantiles calls these two functions
    */
  def _PyGetQuantilesInt(
    x: JavaRDD[Int],
    quantiles: ArrayList[Double],
    epsilon: Double
  ): Array[Int] = getQuantiles(x, quantiles.toSeq, epsilon).toArray

  def _PyGetQuantilesDouble(
    x: JavaRDD[Double],
    quantiles: ArrayList[Double],
    epsilon: Double
  ): Array[Double] = getQuantiles(x, quantiles.toSeq, epsilon).toArray

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
  /*  this is called by _PyGetGroupedQuantilesStringDouble and _PyGetGroupedQuantilesStringInt
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
  private def groupedQuantilesToPython[T](x: RDD[(Any, Map[Double,T])]): JavaRDD[Array[Any]] = {
    val calculatedArrays: RDD[Array[Any]] = x.map(
      (y: (Any, Map[Double, T])) => {
        // this leverages the JavaConverters import
        val y2: java.util.Map[Double, T] = y._2
        Array(y._1, y2)
      }
    )
    /*
    val calculatedArrays: RDD[Array[Any]] = x.map(
      (y: ((Any, Double), T)) => {
        Array(Array(y._1._1, y._1._2), y._2)
      }
    )
    */
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



// beware — the right basis for comparison is usually just going to be on v
// but I think it's still fine to make this a case class
case class GKEntry[T](
  val v: T,
  val g: Long,
  val delta: Long
)(implicit num: Numeric[T]) {
  import num._
}

class GKRecord[T](
  val epsilon: Double,
  val sample: List[GKEntry[T]] = List[GKEntry[T]](),
  val count: Long = 0
)(implicit num: Numeric[T]) extends Serializable {
  import num._
  val compressThreshold: Long = (1.0/(2.0*epsilon)).toLong

  /* defs for listInsert and listReplace were moved to the package object! */

  def insert(v: T): GKRecord[T] = {
    val newSample: List[GKEntry[T]] = {
      if (
        (sample.length == 0) ||
        (v < sample.head.v)
      ) {
        //sample.insert(0, GKEntry(v, 1, 0))
        GKEntry(v,1,0) +: sample
        // catch the edge case where v is greater than any value in sample here.
      } else if (v >= sample.last.v) {
        //sample.insert(sample.length, GKEntry(v, 1, 0))
        sample :+ GKEntry(v,1,0)
      } else {
        val i: Int = sample.indexWhere(
          (g: GKEntry[T]) => v < g.v
        )
        val delta: Long = if (count < compressThreshold) {
          0L
          //math.max(math.floor(2*epsilon*count).toLong - 1, 0L)
        } else {
          //math.floor(2*epsilon*count).toLong
          val a = math.max(math.floor(2*epsilon*count).toLong - 1L, 0L)
          //val b = sample(i).g + sample(i).delta - 1
          //if (b > a) println(s"$a $b")
          //sample(i).g + sample(i).delta - 1
          a
        }
        //sample.insert(i, GKEntry(v, 1, delta))
        listInsert(sample, i, GKEntry[T](v, 1, delta))
      }
    }

    val newRecord: GKRecord[T] = new GKRecord[T](
      epsilon,
      newSample,
      count + 1
    )
    if (newRecord.count % compressThreshold == 0) {
      newRecord.compress
    } else {
      newRecord
    }
  }

  def compress(): GKRecord[T] = {
    /*  there should be some documentation here, but it
        does, in principle, the same thing the previous one does
    */

    val threshold: Long = math.floor(2*epsilon*count).toLong
    /** each of these functions are called exactly once.
      * Not sure if this makes my code more readable or less.
      */
    def isCombinable(a: GKEntry[T], b: GKEntry[T]): Boolean = {
        (a.g + b.g + b.delta) < threshold
    }
    def combine(a: GKEntry[T], b: GKEntry[T]): GKEntry[T] = {
      GKEntry(b.v, a.g+b.g, b.delta)
    }
    // remember that tailrec just exists to get a compiler error if the
    // function isn't tail-recursive. The compiler will find and optimize
    // tail-recursive functions with or without this annotation.
    /** This works through the samples one at a time.
      * If the first entry can be combined with the second, it does so, and
      * considers the "new" first entry against the "new" second entry (so,
      * it used to be the third entry.)
      * If the first twon entries can't be combined, it places the first entry
      * at the end of the output list and then considers the next two.
      *
      */
    @tailrec
    def collapse(
      head: GKEntry[T],
      tail: List[GKEntry[T]],
      out: List[GKEntry[T]] = Nil
    ): List[GKEntry[T]] = {
      /** we never remove the last element from the sample so once tail is
        * down to a single entry, we're done.
        * Note that the tail.isEmpty check should never be true in practice,
        * because if there are only two elements in the sample collapse
        * won't even get called (se below). But I left it this way because any
        * runtime error I can avoid is a runtime error I want to avoid.
        * In python I would surely just run
        *     if len(tail) < 2:
        * but I presume that's slower since it runs the whole length of the list.
        */
      if (tail.isEmpty || tail.tail.isEmpty) {
        (out :+ head) ++ tail
      } else if (isCombinable(head, tail.head)) {
        collapse(
          combine(head, tail.head),
          tail.tail,
          out
        )
      } else {
        // not combinable so append previous acc and
        // make the new one
        collapse(
          tail.head,
          tail.tail,
          out :+ head
        )
      }
    }

    val out: List[GKEntry[T]] = {
      // if there are zero, one, or two elements in sample we can't possibly
      // compress (because you never modify the first or last element of sample)
      // would it be better to do
      // if ((sample.isEmpty || sample.tail.isEmpty) || sample.tail.tail.isEmpty)
      // ?
      // It probably would but this is only called once per compression.
      if (sample.length <= 2) sample
      else {
        val head: GKEntry[T] = sample.head
        val tail: List[GKEntry[T]] = sample.tail
        head +: collapse(tail.head, tail.tail)
        // we should really never be down here.
      }
    }
    (new GKRecord[T](epsilon, out, count))
  }

  def query(quantile: Double): T = {
    /*  Ranks run from 1 to count and NOT from 0 to count-1
        An alternative construction of desiredRank would be
        ceil(quantile*count)
        This has the advantage of rounding a desiredRank of 7.2 to 7 and not 8,
        But it has the disadvantage of rounding 0.4 to 0, hence the math.max
        bit. For any reasonably-large data set sampled at any normal resolution,
        these should really do the exact same thing.
    */
    val desiredRank: Long = math.max(math.round(quantile * (count)).toLong, 1L)
    // I had a ceiling function around this and I think that was wrong.
    val rankEpsilon: Double = epsilon * count
    // the tail is to drop the leading 0 added by scanLeft
    // scanLeft takes the cumulative sum (at least it does
    // does when the combine op is addition :)
    val startingRanks: Seq[Long] = sample.map(_.g).scanLeft(0L)(_ + _).tail
    val endingRanks: Seq[Long] = startingRanks.zip(sample).map({
      case (a: Long, b: GKEntry[T]) => a+b.delta
    })
    val idx: Int = startingRanks.zip(endingRanks).indexWhere({
      case (startingRank: Long, endingRank: Long) => (
        (desiredRank-startingRank) <= rankEpsilon &&
        (endingRank-desiredRank) <= rankEpsilon
      )
    })
    // if nothing matches, we have a problem.
    if (idx < 0) {
      throw new Exception(s"Could not find desiredRank $desiredRank quantile $quantile count $count given rankEpsilon $rankEpsilon, for startingRanks $startingRanks, endingRank $endingRanks")
    }
    sample(idx).v
  }

  def combine(that: GKRecord[T]): GKRecord[T] = {
    if (this.sample.isEmpty) that
    else if (that.sample.isEmpty) this
    else {
      // this is complicated and I should explain it.
      // The paper is Greenwald-Khanna 2004,
      // Power-Conserving Computation of Order-Statistics over Sensor Networks
      // exploiting the fact that GKEntries are case classes
      // so I can use them as keys in a Map
      // this is a LOT better than that Python version I wrote! (if it works)
      // we need to keep track of the next entry in the other list
      // so we can recalculate delta on an entry-by-entry basis.
      // I include the this's here just so I can keep track of what I'm doing.
      val otherNext: Map[GKEntry[T], Option[GKEntry[T]]] =
        this.sample.map(
          (x: GKEntry[T]) => (x -> that.sample.find((y) => y.v > x.v))
        ).toMap ++
        that.sample.map(
          (x: GKEntry[T]) => (x -> this.sample.find((y) => y.v > x.v))
        ).toMap
      val combined: List[GKEntry[T]] = (sample ::: that.sample).sortBy(_.v)
      val out: List[GKEntry[T]] = combined.map(
        (x: GKEntry[T]) => otherNext(x) match {
          case None => x
          case Some(otherNext) => {
            val newDelta: Long = x.delta + otherNext.delta + otherNext.g -1
            x.copy(delta=newDelta)
          }
        }
      )

      // no code that I've written would ever cause epsilon and that.epsilon to be different.
      val newEpsilon: Double = math.max(epsilon, that.epsilon)
      val oldCount: Long = math.max(count, that.count)
      val newCount: Long = count + that.count
      val toReturn: GKRecord[T] = new GKRecord[T](
        newEpsilon,
        out.toList,
        newCount
      )
      /*
         This new check attempts to see whether we've crossed over a threshold
         by seeing how many times we ought to have compressed before combining records
         and comparing it to how many times we ought to have compressed after it,
         assuming that we're adding the smaller record to the bigger one.

         So if the compression threshold is 50 and we add a 48-sample list to a
         3-sample list, it will compress (48/50 != 51/50), but if we add
         a 51-sample list and a 40-sample list it won't (51/50 == 91/50)
      */
      // these are Longs so it's floor division, which is what we want.
      val newCompressionThreshold: Long = (1.0/(2.0*newEpsilon)).toLong
      if (
        oldCount/newCompressionThreshold !=
        newCount/newCompressionThreshold
      ) {
        toReturn.compress
      } else {
        toReturn
      }
    }
  }

}
