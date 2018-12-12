package com.github.nlzimmerman

import collection.mutable.ListBuffer

// For now this is a line-for-line rewrite of what I did in Python;
// Then I'll move it into a class

object GKQuantile {
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.rdd.PairRDDFunctions
  // oof
  // https://stackoverflow.com/questions/16921168/scala-generic-method-no-classtag-available-for-t
  import scala.reflect.ClassTag
  def getQuantiles(
    x: Seq[Double],
    quantiles: Seq[Double],
    epsilon: Double=0.01
  ): Seq[Double] = {
    val d: GKRecord = x.foldLeft(new GKRecord(epsilon))(
      (x: GKRecord, y: Double) => x.insert(y)
    )
    quantiles.map((q: Double) => d.query(q))
  }

  def getQuantiles(
    x: RDD[Double],
    quantiles: Seq[Double],
    epsilon: Double
  ): Seq[Double] = {
    val d: GKRecord = x.treeAggregate(
      new GKRecord(epsilon)
    )(
      (a: GKRecord, b: Double) => a.insert(b),
      (a: GKRecord, b: GKRecord) => a.combine(b)
    )
    quantiles.map((q: Double) => d.query(q))
  }

  def getGroupedQuantiles[T: ClassTag](
    r: RDD[(T, Double)],
    quantiles: Seq[Double],
    epsilon: Double = 0.01
  ): PairRDDFunctions[(T, Double), Double] = {
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
    ).map(
      // shifts from (key, (quantile, value)) to
      // ((key, quantile), value)
      (x: (T, (Double, Double))) => ((x._1, x._2._1), x._2._2)
    )
  }
}

// beware — the right basis for comparison is usually just going to be on v
// but I think it's still fine to make this a case class
case class GKEntry(
  val v: Double,
  val g: Long,
  val delta: Long
)

class GKRecord(
  val epsilon: Double,
  val sample: List[GKEntry] = List[GKEntry](),
  val count: Long = 0,
) extends Serializable {
  val compressThreshold: Long = (1.0/(2.0*epsilon)).toLong

  def listInsert[T](l: List[T], i: Int, a: T): List[T] = {
    (l.dropRight(l.length-i) :+ a) ::: l.drop(i)
  }
  def insert(v: Double): GKRecord = {
    val newSample: List[GKEntry] = {
      if (
        (sample.length == 0) ||
        (v < sample.head.v)
      ) {
        //sample.insert(0, GKEntry(v, 1, 0))
        listInsert(sample.toList, 0, GKEntry(v,1,0))
        // catch the edge case where v is greater than any value in sample here.
      } else if (v > sample.last.v) {
        //sample.insert(sample.length, GKEntry(v, 1, 0))
        listInsert(sample.toList, sample.length, GKEntry(v,1,0))
      } else {
        val i: Int = sample.indexWhere(
          (g: GKEntry) => v < g.v
        )
        if (i < 1) {
          throw new Exception("i should be greater than 1")
        }
        val delta: Long = math.floor(2*epsilon*count).toLong
        //sample.insert(i, GKEntry(v, 1, delta))
        listInsert(sample.toList, i, GKEntry(v, 1, delta))
      }
    }

    val newRecord: GKRecord = new GKRecord(
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
  def compress(): GKRecord = {
    var i: Int = 1
    val out: ListBuffer[GKEntry] = sample.to[ListBuffer].clone
    while (i < out.length-1) {
      if (
        (
          out(i).g + out(i+1).g + out(i+1).delta
        ) < math.floor(2*epsilon*count)
      ) {
        out(i+1) = out(i+1).copy(g=(out(i+1).g+out(i).g))
        //sample(i+1).g += sample(i).g
        out.remove(i)
      } else {
        i += 1
      }
    }
    (new GKRecord(epsilon, out.toList, count))
  }
  def query(quantile: Double): Double = {
    val desiredRank: Long = math.ceil(quantile * (count - 1)).toLong
    val rankEpsilon: Double = epsilon * count
    // the tail is to drop the leading 0 added by scanLeft
    // scanLeft takes the cumulative sum (at least it does
    // does when the combine op is addition :)
    val startingRanks: Seq[Long] = sample.map(_.g).scanLeft(0L)(_ + _).tail
    val endingRanks: Seq[Long] = startingRanks.zip(sample).map({
      case (a: Long, b: GKEntry) => a+b.delta
    })
    val idx: Int = startingRanks.zip(endingRanks).indexWhere({
      case (startingRank: Long, endingRank: Long) => (
        (desiredRank-startingRank) <= rankEpsilon &&
        (endingRank-desiredRank) <= rankEpsilon
      )
    })
    sample(idx).v
  }

  def combine(that: GKRecord): GKRecord = {
    if (this.sample.length == 0) that
    else if (that.sample.length == 0) this
    else {
      val thisSample: ListBuffer[GKEntry] = sample.to[ListBuffer]
      val thatSample: ListBuffer[GKEntry] = that.sample.to[ListBuffer]
      val out: ListBuffer[GKEntry] = new ListBuffer[GKEntry]
      while (thisSample.length > 0 && thatSample.length > 0) {
        // This could be much, much better.
        out += {
          val (thisElement: GKEntry, otherNextElement: GKEntry) = {
            if (thisSample(0).v < thatSample(0).v) {
              // I'm not sure if it's safe to not copy this in Spark.
              // it should be fine in straight Scala.
              val a: GKEntry = thisSample.remove(0)
              val b: GKEntry = thatSample.find(
                (x) => x.v > a.v
              ).get // will throw an error if there is none. That is correct
              (a, b)
            } else {
              val a: GKEntry = thatSample.remove(0)
              // there was a typo here where I did
              // thisSample.remove(0)
              // Which gives a runtime error.

              val b: GKEntry = thisSample.find(
                (x) => x.v > a.v
              ).get
              (a, b)
            }
          }
          val newDelta: Long = thisElement.delta +
            otherNextElement.delta + otherNextElement.g - 1
          //thisElement.delta = newDelta
          thisElement.copy(delta=newDelta)
        }
      } // either thisSample or thatSample has been exhausted now
      // again, I'm of the impression that it's NOT safe to NOT copy
      // thisSample and thatSample and just do a ++=
      // which is why I'm doing this.
      while (thisSample.length > 0) {
        out += thisSample.remove(0)
      }
      while (thatSample.length > 0) {
        out += thatSample.remove(0)
      }
      val newEpsilon: Double = math.max(epsilon, that.epsilon)
      val countIncrease: Long = math.min(count, that.count)
      val newCount: Long = count + that.count
      val toReturn: GKRecord = new GKRecord(
        newEpsilon,
        out.toList,
        newCount
      )
      if (countIncrease >= math.floor(1.0/(2.0*newEpsilon))) {
        toReturn.compress
      } else {
        toReturn
      }
    }
  }
}
