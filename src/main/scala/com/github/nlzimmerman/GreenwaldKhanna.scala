package com.github.nlzimmerman

// _ imports aren't a great way to learn a language but I use the implicits all the time.
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext._

import scala.annotation.tailrec

/** The logic here is defined in

    1. Greenwald, Michael, and Sanjeev Khanna. "Space-efficient online computation
    of quantile summaries." ACM SIGMOD Record. Vol. 30. No. 2. ACM, 2001.

    and

    2. Greenwald, Michael B., and Sanjeev Khanna. "Power-conserving computation of
    order-statistics over sensor networks." Proceedings of the twenty-third ACM
    SIGMOD-SIGACT-SIGART symposium on Principles of database systems. ACM, 2004.

    Both are pretty easy to follow once you get the notation down. The first paper
    does most of the work, the second one implements a combine operation.

    BE WARNED: This
  */

// this is just the tuple that represents each entry in the record.
case class GKEntry[T](
  val v: T,
  val g: Long,
  val delta: Long
)/*(implicit num: Numeric[T]) {
  import num._
}*/

// this is the record itself. Use insert(), combine(), and query() to manipulate it.
// you should only instantiate this with epsilon publicly,
// specifying sample and count only makes sense for updates.
class GKRecord[T](
  val epsilon: Double,
  val sample: List[GKEntry[T]] = List[GKEntry[T]](),
  val count: Long = 0
)(implicit num: Numeric[T]) extends Serializable {
  import num._
  val compressThreshold: Long = (1.0/(2.0*epsilon)).toLong
  // the INSERT operation defined in the first paper.
  def insert(v: T): GKRecord[T] = {
    val newSample: List[GKEntry[T]] = {
      if ( // if v is smaller than any item in the record
        (sample.length == 0) ||
        (v < sample.head.v)
      ) {
        GKEntry(v,1,0) +: sample
      } else if (v >= sample.last.v) { // if the v is larger than any item record
        //sample.insert(sample.length, GKEntry(v, 1, 0))
        sample :+ GKEntry(v,1,0)
      } else {  // all other cases, we insert v into the position i where, after insertion
                // entry(i-1) <= entry(i) < entry(i+1)
        val i: Int = sample.indexWhere(
          (g: GKEntry[T]) => v < g.v
        )
        val delta: Long = if (count < compressThreshold) {
          // this solves a bug in the paper.
          // see here https://www.stevenengelhardt.com/2018/03/07/calculating-percentiles-on-streaming-data-part-2-notes-on-implementing-greenwald-khanna/#GK01
          0L
        } else {
          // I used this to solve a specific bug that I encountered earlier that I
          // believe is correctly solved by the check above.
          // math.max(math.floor(2*epsilon*count).toLong - 1L, 0L)
          math.floor(2*epsilon*count).toLong
        }
        // listInsert is defined in the package object.
        listInsert(sample, i, GKEntry[T](v, 1, delta))
      }
    }

    val newRecord: GKRecord[T] = new GKRecord[T](
      epsilon,
      newSample,
      count + 1
    )
    // this is obvious but in case I forget, this check works becuase count only ever
    // increases by 1.
    if (newRecord.count % compressThreshold == 0) {
      newRecord.compress
    } else {
      newRecord
    }
  }

  // this is also intended to be exactly what's in the first paper
  def compress(): GKRecord[T] = {
    /** the floor wouldn't normally be necessary here since 2*epsilon*count is
      * normally an integer when compress is called. That may not be the case, though
      * if compress is called as the result of a combine operation.
      * Where the compressionThreshold defined in the class definition was
      * the integer where, after the count reaches a multiple of it, it's time
      * to recompress, this threshold is essentially the number of those cycles we've
      * been through, which also represents the largest range any entry can safely contain.
      * (if our count is 700 and our epsilon is 0.01, each observation can safely
      * hold up to 14 entries so that any given query will be correct to within
      * +/- 7).
      * You could equally define this as
      *
      * val threshold: Long = count/compressionThreshold
      */
    val threshold: Long = math.floor(2*epsilon*count).toLong
    // As you can see, I didn't actually use this version https://github.com/WladimirLivolis/GreenwaldKhanna/blob/master/src/GK.java
    // but I am in debt to it. Good thing it's MIT licensed!
    def computeBands(delta: Long): Long = {
      // p is the largest value of delta that we can find in the record for this
      // value of count and epsilon
      // the value formerly known as "p" is now just threshold pulled in from out-of-scope
      // no log2 in Scala
      val largestBand: Long = math.ceil(math.log(threshold)/math.log(2)).toLong
      @tailrec
      def checker(alpha: Long): Long =
        if ({
          // making these variables to avoid doing the math twice.
          // what a fun old trick.
          val twoToTheAlpha: Long = 1L << alpha//math.pow(2, alpha).toLong
          val twoToTheAlphaMinusOne: Long = 1L << (alpha-1)//math.pow(2, alpha-1).toLong
          (
            (threshold-twoToTheAlpha-(threshold%twoToTheAlpha)) < delta &&
            (threshold-twoToTheAlphaMinusOne-(threshold%twoToTheAlphaMinusOne)) >= delta
          )
        }) alpha
        else checker(alpha-1)
      if(delta==threshold) 0 else checker(largestBand)
    }
    // doing this to avoid actually doing the math more than once.
    // I'm using a Map and not a Vector because Vector keys are supposed to be ints
    // and I'm using Longs. It may be silly for me to be using Longs.
    val band: Vector[Long] =
      (0L to threshold).map(
        (x: Long) => computeBands(x)
      ).toVector
    /** each of these functions are called exactly once.
      * Not sure if this makes my code more readable or less.
      */
    /** it's safe to combine two entries if the new entry that would be made by
      * doing so would contain fewer than the threshold (above) number of values.
      */
    def isCombinable(a: GKEntry[T], b: GKEntry[T]): Boolean = {
        (band(a.delta.toInt) < band(b.delta.toInt)) &&
        ((a.g + b.g + b.delta) < threshold)
    }
    def combine(a: GKEntry[T], b: GKEntry[T]): GKEntry[T] = {
      GKEntry(b.v, a.g+b.g, b.delta)
    }
    // remember that tailrec just exists to get a compiler error if the
    // function isn't tail-recursive. The compiler will find and optimize
    // tail-recursive functions with or without this annotation.
    /** This works through the samples one at a time.
      * If the first entry can be combined with the second, it does so, and
      * considers the "new" first entry against the "new" second entry (i.e.,
      * it used to be the third entry.)
      * If the first two entries can't be combined, it places the first entry
      * at the end of the output list and then considers the next two.
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
        * won't even get called (see below). But I left it this way because any
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
        // not combinable so append the old first element to the output and
        // consider the new head (old second element) and the old third element
        collapse(
          tail.head,
          tail.tail,
          out :+ head
        )
      }
    }
    // this is where the work is actually done.
    val out: List[GKEntry[T]] = {
      /** if there are zero, one, or two elements in sample we can't possibly
        * compress (because you never modify the first or last element of sample)
        * would it be better to do
        * if ((sample.isEmpty || sample.tail.isEmpty) || sample.tail.tail.isEmpty)
        * ?
        * It probably would but this is only called once per compression.
        */
      if (sample.length <= 2) sample
      else {
        // never attempt to combine the first element.
        // (the logic that saves us from combining the last two is in collapse)
        val head: GKEntry[T] = sample.head
        val tail: List[GKEntry[T]] = sample.tail
        head +: collapse(tail.head, tail.tail)
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
    val desiredRank: Long = math.max(math.round(quantile * count).toLong, 1L)
    // I had a ceiling function around this early — wrong.
    // (but the bug produced by that only happens at small counts)
    val rankEpsilon: Double = epsilon * count
    // the starting rank is the cumulative sum of record.g values.
    // this just takes a cumulative sum.
    // tail used here is to drop the leading 0 added by scanLeft
    val startingRanks: Seq[Long] = sample.map(_.g).scanLeft(0L)(_ + _).tail
    // the ending rank of a given entry is the starting rank of the same entry
    // plus the delta value.
    val endingRanks: Seq[Long] = startingRanks.zip(sample).map({
      case (a: Long, b: GKEntry[T]) => a+b.delta
    })
    // this actually finds the place where there's a match.
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
  // This comes from the second paper.
  def combine(that: GKRecord[T]): GKRecord[T] = {
    if (this.sample.isEmpty) that
    else if (that.sample.isEmpty) this
    else {
      // when we combine to samples,
      // we need to keep track of the next entry in the other list
      // so we can recalculate delta on an entry-by-entry basis.
      // I include the this's here just so I can keep track of what I'm doing.
      val otherNext: Map[GKEntry[T], Option[GKEntry[T]]] =
        // do it for this.sample
        this.sample.map(
          (x: GKEntry[T]) => (x -> that.sample.find((y) => y.v > x.v))
        ).toMap ++
        // and for that.sample.
        that.sample.map(
          (x: GKEntry[T]) => (x -> this.sample.find((y) => y.v > x.v))
        ).toMap
      // actually combine the samples.
      // and use the otherNext map to recalcualte deltas.
      // this is documented in section 3.1.3 but I had to do some algebra to
      // get this, and I've since lost my notes.
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
      // oldCount is used to decide if we need to trigger a compression.
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
