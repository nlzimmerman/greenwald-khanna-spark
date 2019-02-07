package com.github.nlzimmerman

object DirectQuantile {
  def getQuantiles[T](x: Seq[T], quantiles: Seq[Double])
      (implicit num: Numeric[T]): Map[Double, T] = {
    import num._
    val s: Seq[T] = x.sorted
    val l: Long = s.length
    val targetRanks: Map[Double, Int] = quantiles.map(
      // math.ceil((seq.length - 1) * (p / 100.0)).toInt
      // (x) => Math.ceil((l-1)*x).toInt
      (quantile: Double) => quantile -> (math.max(math.round(quantile * l).toInt, 1)-1)
    ).toMap
    targetRanks.mapValues(s(_))
  }

}
