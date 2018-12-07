package com.github.nlzimmerman

// For now this is a line-for-line rewrite of what I did in Python;
// Then I'll move it into a class

class GKEntry(val v: Double, var g: Long, val delta: Long) {
  override def toString: String = s"($v, $g, $delta)"
}

class GKRecord(val epsilon: Double) {
  val sample: collection.mutable.ListBuffer[GKEntry] =
    new collection.mutable.ListBuffer[GKEntry]
  var count: Int = 0
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
}

object GreenwaldKhannaClassless {

}
