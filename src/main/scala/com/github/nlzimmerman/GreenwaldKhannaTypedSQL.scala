package com.github.nlzimmerman

// here goes!
// cribbing from spark/examples/src/main/scala/org/apache/spark/examples/sql/UserDefinedTypedAggregation.scala
// and alsio https://docs.databricks.com/spark/latest/dataframes-datasets/aggregators.html

import org.apache.spark.sql.expressions.Aggregator

import org.apache.spark.sql.{
  Encoder,
  Encoders
}

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
