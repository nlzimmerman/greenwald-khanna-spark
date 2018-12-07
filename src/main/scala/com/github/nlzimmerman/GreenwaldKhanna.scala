package com.github.nlzimmerman

import collection.mutable.ListBuffer

// For now this is a line-for-line rewrite of what I did in Python;
// Then I'll move it into a class

class GKEntry(val v: Double, var g: Long, var delta: Long) {
  override def toString: String = s"($v, $g, $delta)"
  def copy(): GKEntry = new GKEntry(v, g, delta)
}

class GKRecord(
  val epsilon: Double,
  val sample: ListBuffer[GKEntry] = new ListBuffer[GKEntry],
  var count: Long = 0,
) {
  val compressThreshold: Long = (1.0/(2.0*epsilon)).toLong

  def insert(v: Double): Unit = {
    if (
      (sample.length == 0) ||
      (v < sample.head.v)
    ) {
      sample.insert(0, new GKEntry(v, 1, 0))
      // catch the edge case where v is greater than any value in sample here.
    } else if (v > sample.last.v) {
      sample.insert(sample.length, new GKEntry(v, 1, 0))
    } else {
      val i: Int = sample.indexWhere(
        (g: GKEntry) => v < g.v
      )
      if (i < 1) {
        throw new Exception("i should be greater than 1")
      }
      val delta: Long = math.floor(2*epsilon*count).toLong
      sample.insert(i, new GKEntry(v, 1, delta))
    }
    count += 1
    if (count % compressThreshold == 0) {
      compress
    }

  }
  def compress(): Unit = {
    var i: Int = 1
    while (i < sample.length-1) {
      if (
        (
          sample(i).g + sample(i+1).g + sample(i+1).delta
        ) < math.floor(2*epsilon*count)
      ) {
        val ss = sample(i)
        sample(i+1).g += sample(i).g
        sample.remove(i)
      } else {
        i += 1
      }
    }
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
              val a: GKEntry = thisSample(0).copy
              thisSample.remove(0)
              val b: GKEntry = thatSample.find(
                (x) => x.v > a.v
              ).get // will throw an error if there is none. That is correct
              (a, b)
            } else {
              val a: GKEntry = thatSample(0).copy
              thisSample.remove(0)
              val b: GKEntry = thisSample.find(
                (x) => x.v > a.v
              ).get
              (a, b)
            }
          }
          val newDelta: Long = thisElement.delta + otherNextElement.delta +
            otherNextElement.g - 1
          thisElement.delta = newDelta
          thisElement
        }
      } // either thisSample or thatSample has been exhausted now
      // again, I'm of the impression that it's NOT safe to NOT copy
      // thisSample and thatSample and just do a ++=
      // which is why I'm doing this.
      while (thisSample.length > 0) {
        out += thisSample(0).copy
        thisSample.remove(0)
      }
      while (thatSample.length > 0) {
        out += thatSample(0).copy
        thatSample.remove(0)
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
      }
      toReturn
    }
  }
}

object GreenwaldKhannaClassless {

}
