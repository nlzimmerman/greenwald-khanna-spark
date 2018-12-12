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
  val sample: ListBuffer[GKEntry] = new ListBuffer[GKEntry],
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
    val newCount: Long = count + 1

    val newRecord: GKRecord = new GKRecord(
      epsilon,
      newSample.to[ListBuffer],
      newCount
    )
    if (count % compressThreshold == 0) {
      newRecord.compress
    } else {
      newRecord
    }
  }
  def compress(): GKRecord = {
    var i: Int = 1
    var out: ListBuffer[GKEntry] = sample.to[ListBuffer].clone
    while (i < out.length-1) {
      if (
        (
          out(i).g + out(i+1).g + out(i+1).delta
        ) < math.floor(2*epsilon*count)
      ) {
        out(i+1) = out(i+1).copy(g=(sample(i+1).g+sample(i).g))
        //sample(i+1).g += sample(i).g
        out.remove(i)
      } else {
        i += 1
      }
    }
    /*
    while (i < sample.length-1) {
      val first: GKEntry = sampleList(i)
      val remainder: Seq[GKEntry] = sampleList.drop(i+1).toSeq
      // there is probably a faster but still functional
      // way to do this
      // this goes 1, 2, 3… remainder.length
      // so that slice won't need to have 1 added
      val ranges: IndexedSeq[Long] = (1 to remainder.length).map(
        (i: Int) => {
          // just the first N elements of sampleList
          val sub: Seq[GKEntry] = remainder.slice(0, i)
          first.g + sub.map(_.g).reduce(_ + _) + sub.last.delta
        }
      )
      val stride: Int = ranges.lastIndexWhere((x: Long) => x < math.floor(2*epsilon*count))
      if (stride == -1) {
        // nothing to combine
        out += first
      } else {
        val last: GKEntry = remainder(stride)
        val combinedRecord: GKEntry = GKEntry(
          last.v,
          remainder.slice(0, stride+1).map(_.g).reduce(_ + _),
          last.delta
        )
        out += combinedRecord
      }
      // plus one because we are incrementing, plus one because
      // remainder does not include the first element
      i += (stride + 2)
    }
    */
    new GKRecord(
      epsilon,
      out,
      count
    )

  }
  def query(quantile: Double): Double = {
    val desired_rank: Long = math.ceil(quantile * (count - 1)).toLong
    val rank_epsilon: Double = epsilon * count
    var starting_rank: Long = 0
    // it's possible to do this without a while loop; I just haven't
    // gotten there yet.
    // This is not good practice in Scala, just a direct rewrite of the Python
    // for now.
    var i: Int = 0
    var toReturn: Double = Double.NegativeInfinity
    var break: Boolean = false
    while (i < sample.length && !break) {
      starting_rank += sample(i).g
      val ending_rank: Long = starting_rank + sample(i).delta
      if (
        ((desired_rank-starting_rank) <= rank_epsilon) &&
        ((ending_rank-desired_rank) <= rank_epsilon)
      ) {
          toReturn = sample(i).v
          break = true
      } else {
        i += 1
      }
    }
    toReturn
  }

  def combine(that: GKRecord): GKRecord = {
    if (this.sample.length == 0) that
    else if (that.sample.length == 0) this
    else {
      val thisSample: ListBuffer[GKEntry] = sample.clone
      val thatSample: ListBuffer[GKEntry] = that.sample.clone
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
        out,
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
