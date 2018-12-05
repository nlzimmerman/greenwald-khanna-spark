package com.github.nlzimmerman

// For now this is a line-for-line rewrite of what I did in Python;
// Then I'll move it into a class

class GKEntry(val v: Double, val g: Long, var delta: Long)

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
      val delta: Long = Math.floor(2*epsilon*count).toLong
      sample.insert(i, new GKEntry(v, 1, delta))
    }
    count += 1
    if (count % compressThreshold == 0) {
      () //run compress, which I haven't written
    }

  }
}

object GreenwaldKhannaClassless {

}
