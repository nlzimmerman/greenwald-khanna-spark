package com.github.nlzimmerman

// here goes!
// cribbing from spark/examples/src/main/scala/org/apache/spark/examples/sql/UserDefinedTypedAggregation.scala
// and alsio https://docs.databricks.com/spark/latest/dataframes-datasets/aggregators.html

import org.apache.spark.sql.{
  Encoder,
  Encoders,
  SparkSession
}
import org.apache.spark.sql.expressions.{
  Aggregator,
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  DataType,
  LongType,
  DoubleType,
  StructType,
  StructField,
  ArrayType,
  MapType
}

import scala.collection.JavaConversions._

// I don't fully understand what the : Encoder part does but it needs to be there.
class GKAggregator[T: Numeric: Encoder](
  quantiles: Seq[Double],
  epsilon: Double
) extends Aggregator[T, GKRecord[T], Map[Double,T]] with Serializable {
  private val numeric = implicitly[Numeric[T]]

  def zero: GKRecord[T] = new GKRecord[T](epsilon)
  def reduce(a: GKRecord[T], b: T): GKRecord[T] = a.insert(b)
  def merge(a: GKRecord[T], b: GKRecord[T]): GKRecord[T] = a.combine(b)
  def finish(reduction: GKRecord[T]): Map[Double,T] = quantiles.map(
    (q: Double) => (q -> reduction.query(q))
  ).toMap
  // not sure if I'm doing this right.
  def bufferEncoder: Encoder[GKRecord[T]] = Encoders.kryo[GKRecord[T]]
  def outputEncoder: Encoder[Map[Double,T]] = Encoders.kryo[Map[Double,T]]
}

/** so, that was cool but it doesn't work with Python as far as I know beceause
  * so far as I've been able to determine, Pyspark doesn't support TypedColumns
  * soooooo, let's see about this.
  *
  * This works fine and is, so far as I know, not wrong, but it may be very much
  * less efficient than an implementation that actually uses ArrayBuffers
  * and Rows the way they're "supposed" to be use with updates etc.
  *
  * This converts our buffers into an instance typed GKRecord,
  * does the insert or combine logic on that, and then turns it back into a buffer.
  * I have no idea how expensive that is but I wouldn't presume that it's cheap.
  */
class UntypedGKAggregator(
  val quantiles: Seq[Double],
  val epsilon: Double
) extends UserDefinedAggregateFunction with Serializable {
  // for python compatibility since py4j makes python lists into
  // java.util.ArrayList
  def this(
    q: java.util.ArrayList[Double],
    e: Double
  ) = this(
    {
      // leveraging implicit conversion in scala.collection.JavaConversions._
      val l: Seq[Double] = q
      l
    },
    e
  )
  // it's possible I'm wrong and this IS deterministic. There's no randomness
  // but the order that
  def deterministic: Boolean = false
  // this is the return type.
  def dataType: DataType = MapType(DoubleType, DoubleType)
  // this is the input type. Just one column. Note that the actual name of the column
  // doesn't matter; this is just what it's referred to here. 
  def inputSchema: StructType = StructType(
    StructField("value", DoubleType, false) ::
    Nil
  )

  // equivalent to a GKEntry
  val recordSchema: StructType = StructType(
    StructField("value", DoubleType, false) ::
    StructField("g", LongType, false) ::
    StructField("delta", LongType, false) ::
    Nil
  )
  // this has to do everything GKRecord does, but do it with spark SQL
  // logic
  def bufferSchema: StructType = StructType(
    StructField("sample", ArrayType(recordSchema)) ::
    StructField("count", LongType) ::
    Nil
  )
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Any]()
    buffer(1) = 0L
  }

  // FROM HERE DOWN ARE FUNCTIONS THAT WOULD IDEALLY BE REWRITTEN FOR PERFORMANCE.

  /** Instead of actually rewriting this in Spark SQL, I'm just
    * writing a wrapper that calls the code I've already written.
    * I don't think this is likely to be the "right" way of doing this
    * since I go from Arrays to GKRecords every single time, which I
    * presume adds a lot of overhead. But this lets me reuse the code
    * that I've already written.
    */
  // Turns the buffer into a typed record.
  def bufferToGKRecord(
    buffer: Row // This Row is of the "type" bufferSchema.
  ): GKRecord[Double] = {
    val count: Long = buffer.getLong(1)
    val sample: Seq[GKEntry[Double]] = {
      val sampleAsList: Seq[Row] = buffer.getSeq[Row](0)
      sampleAsList.map(
        // this Row is of "type" recordSchema
        (x: Row) => GKEntry(
          x.getDouble(0),
          x.getLong(1),
          x.getLong(2)
        )
      )
    }
    new GKRecord[Double](epsilon, sample.toList, count)
  }
  // Turns a typed record back into the stuff that goes into a bufferSchema.
  // This doesn't produce a Row/MutableAggregationBuffer since
  // it seems to be customary to overwrite what's in the current one as a
  // side-effect, so it just produces the stuff that would go into it.
  def gkRecordToBufferElements(r: GKRecord[Double]): (Array[Row], Long) = {
    val newSample: Array[Row] = r.sample.map(
      // the Row produced here is of "type" recordSchema
      (x: GKEntry[Double]) => Row(
        x.v,
        x.g,
        x.delta
      )
    ).toArray
    val count: Long = r.count
    (newSample, count)
  }
  // buffer is of "type" bufferSchema
  // input is of "type" inputSchema
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val newValue: Double = input.getDouble(0)
    val oldGKRecord: GKRecord[Double] = bufferToGKRecord(buffer)
    val updatedGKRecord: GKRecord[Double] = oldGKRecord.insert(newValue)
    val (s: Array[Row], c: Long) = gkRecordToBufferElements(updatedGKRecord)
    buffer(0) = s
    buffer(1) = c
  }
  // buffer1 and buffer2 are both of "type" bufferSchema
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val gkRecord1: GKRecord[Double] = bufferToGKRecord(buffer1)
    val gkRecord2: GKRecord[Double] = bufferToGKRecord(buffer2)
    val merged: GKRecord[Double] = gkRecord1.combine(gkRecord2)
    val (s: Array[Row], c: Long) = gkRecordToBufferElements(merged)
    buffer1(0) = s
    buffer1(1) = c
  }
  // buffer is of "type" bufferSchema
  def evaluate(buffer: Row): Map[Double, Double] = {
    val gkRecord: GKRecord[Double] = bufferToGKRecord(buffer)
    quantiles.map(
      (q: Double) => (q -> gkRecord.query(q))
    ).toMap
  }




}
