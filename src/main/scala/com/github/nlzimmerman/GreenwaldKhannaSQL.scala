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
  ArrayType
}

// I don't fully understand what the : Encoder party does but it needs to be there.
class GKAggregator[T: Numeric: Encoder](
  quantile: Double,
  epsilon: Double
) extends Aggregator[T, GKRecord[T], T] with Serializable {

  private val numeric = implicitly[Numeric[T]]

  def zero: GKRecord[T] = new GKRecord[T](epsilon)
  def reduce(a: GKRecord[T], b: T): GKRecord[T] = a.insert(b)
  //def reduce(a: GKRecord[T], b: T): GKRecord[T] = a.insert(b)
  def merge(a: GKRecord[T], b: GKRecord[T]): GKRecord[T] = a.combine(b)
  def finish(reduction: GKRecord[T]): T = reduction.query(quantile)
  // not sure if I'm doing this right.
  def bufferEncoder: Encoder[GKRecord[T]] = Encoders.kryo[GKRecord[T]]
  def outputEncoder: Encoder[T] = implicitly[Encoder[T]]
}

// so, that was cool but it doesn't work with Python as far as I know beceause
// so far as I've been able to determine, Pyspark doesn't support TypedColumns
// soooooo, let's see about this.

class UntypedGKAggregator(
  val quantile: Double,
  val epsilon: Double
) extends UserDefinedAggregateFunction {
  def deterministic: Boolean = false
  def dataType: DataType = DoubleType
  def inputSchema: StructType = StructType(
    StructField("value", DoubleType, false) ::
    Nil
  )

  // equivalent to a GKEntry
  val recordType: StructType = StructType(
    StructField("value", DoubleType, false) ::
    StructField("g", LongType, false) ::
    StructField("delta", LongType, false) ::
    Nil
  )
  // this has to do everything GKRecord does, but do it with spark SQL
  // logic
  def bufferSchema: StructType = StructType(
    StructField("sample", ArrayType(recordType)) ::
    StructField("count", LongType) ::
    Nil
  )
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Any]()
    buffer(1) = 0L
  }


  def bufferToGKRecord(buffer: Row): GKRecord[Double] = {
    val count: Long = buffer.getLong(1)
    val sample: Seq[GKEntry[Double]] = {
      val sampleAsList: Seq[Row] = buffer.getSeq[Row](0)
      sampleAsList.map(
        (x: Row) => GKEntry(
          x.getDouble(0),
          x.getLong(1),
          x.getLong(2)
        )
      )
    }
    new GKRecord[Double](epsilon, sample.toList, count)
  }
  def gkRecordToBufferElements(r: GKRecord[Double]): (Array[Row], Long) = {
    val newSample: Array[Row] = r.sample.map(
      (x: GKEntry[Double]) => Row(
        x.v,
        x.g,
        x.delta
      )
    ).toArray
    val count: Long = r.count
    (newSample, count)
  }
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    /*  I assume this this is absurdly inefficient but it avoids reusing code,
      * so I'll start with this.
      */
    val newValue: Double = input.getDouble(0)
    val oldGKRecord: GKRecord[Double] = bufferToGKRecord(buffer)
    val printTuple = (oldGKRecord.sample, oldGKRecord.count)
    val updatedGKRecord: GKRecord[Double] = oldGKRecord.insert(newValue)
    val printTuple2 = (updatedGKRecord.sample, updatedGKRecord.count)
    val (s: Array[Row], c: Long) = gkRecordToBufferElements(updatedGKRecord)
    buffer(0) = s
    buffer(1) = c
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val gkRecord1: GKRecord[Double] = bufferToGKRecord(buffer1)
    val gkRecord2: GKRecord[Double] = bufferToGKRecord(buffer2)
    val merged: GKRecord[Double] = gkRecord1.combine(gkRecord2)
    val (s: Array[Row], c: Long) = gkRecordToBufferElements(merged)
    buffer1(0) = s
    buffer1(1) = c
  }
  def evaluate(buffer: Row): Double = {
    val gkRecord: GKRecord[Double] = bufferToGKRecord(buffer)
    gkRecord.query(quantile)
  }




}
