package com.github.nlzimmerman


import scala.annotation.tailrec
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
        GKEntry(v,1,0) +: sample
        // catch the edge case where v is greater than any value in sample here.
      } else if (v > sample.last.v) {
        //sample.insert(sample.length, GKEntry(v, 1, 0))
        sample :+ GKEntry(v,1,0)
      } else {
        val i: Int = sample.indexWhere(
          (g: GKEntry) => v < g.v
        )
        if (i < 1) {
          throw new Exception("i should be greater than 1")
        }
        val delta: Long = math.floor(2*epsilon*count).toLong
        //sample.insert(i, GKEntry(v, 1, delta))
        listInsert(sample, i, GKEntry(v, 1, delta))
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
    /*  there should be some documentation here, but it
        does, in principle, the same thing the previous one does
    */
    val threshold: Long = math.floor(2*epsilon*count).toLong
    def isCombinable(a: GKEntry, b: GKEntry): Boolean = {
        (a.g + b.g + b.delta) < threshold
    }
    def combine(a: GKEntry, b: GKEntry): GKEntry = {
      GKEntry(b.v, a.g+b.g, b.delta)
    }
    @tailrec
    def collapse(
      previous: GKEntry,
      remainder: List[GKEntry],
      acc: List[GKEntry] = Nil,
    ): List[GKEntry] = {
      if (remainder.isEmpty) {
        acc :+ previous
      } else if (isCombinable(previous, remainder.head)) {
        collapse(
          combine(previous, remainder.head),
          remainder.tail,
          acc
        )
      } else {
        // not combinable so append previous acc and
        // make the new one
        collapse(
          remainder.head,
          remainder.tail,
          acc :+ previous
        )
      }
    }
    val out: List[GKEntry] = if (sample.length > 0) {
      collapse(sample.head, sample.tail)
    } else {
      sample
    }
    (new GKRecord(epsilon, out, count))
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
      // exploiting the fact that GKEntries are case classes
      // so I can use them as keys in a Map
      // this is a LOT better than that Python version I wrote! (if it works)
      // we need to keep track of the next entry in the other list
      // so we can recalculate delta on an entry-by-entry basis.
      // I include the this's here just so I can keep track of what I'm doing.
      val otherNext: Map[GKEntry, Option[GKEntry]] =
        this.sample.map(
          (x: GKEntry) => (x -> that.sample.find((y) => y.v > x.v))
        ).toMap ++
        that.sample.map(
          (x: GKEntry) => (x -> this.sample.find((y) => y.v > x.v))
        ).toMap
      val combined: List[GKEntry] = (sample ::: that.sample).sortBy(_.v)
      val out: List[GKEntry] = combined.map(
        (x: GKEntry) => otherNext(x) match {
          case None => x
          case Some(otherNext) => {
            val newDelta: Long = x.delta + otherNext.delta + otherNext.g -1
            x.copy(delta=newDelta)
          }
        }
      )

      val newEpsilon: Double = math.max(epsilon, that.epsilon)
      val countIncrease: Long = math.min(count, that.count)
      val newCount: Long = count + that.count
      val toReturn: GKRecord = new GKRecord(
        newEpsilon,
        out.toList,
        newCount
      )
      /* The old test was
      if (countIncrease >= math.floor(1.0/(2.0*newEpsilon)))
         Which I think is in some sense in that it's
         too conservative about when to compress.
         In principle, we compress every time the count is a multiple of
         1/(2*epsilon) — say, every time the count is a multiple of 50.
         This compresses when the count grows by 50, but, for example,
         It would miss the case where the old count is 48 and the new count is
         51.

         This new check attempts to see whether we've crossed over a threshold
         by seeing how many times we ought to have compressed before combining records
         and comparing it to how many times we ought to have compressed after it.
      */
      // these are Longs so it's floor division, which is what we want.
      val newCompressionThreshold: Long = (1.0/(2.0*newEpsilon)).toLong
      if (
        countIncrease/newCompressionThreshold !=
        newCount/newCompressionThreshold
      ) {
        toReturn.compress
      } else {
        toReturn
      }
    }
  }
}
