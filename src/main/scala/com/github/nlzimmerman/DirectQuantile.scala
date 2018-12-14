package com.github.nlzimmerman

object DirectQuantile {
  def getQuantiles[T](x: Seq[T], quantiles: Seq[Double])
      (implicit num: Numeric[T]): Seq[T] = {
    import num._
    val s: Seq[T] = x.sorted
    val l: Long = s.length
    val targetRanks: Seq[Int] = quantiles.map(
      // math.ceil((seq.length - 1) * (p / 100.0)).toInt
      // (x) => Math.ceil((l-1)*x).toInt
      (quantile: Double) => math.max(math.round(quantile * l).toInt, 1)-1
    )
    targetRanks.map(s(_))
  }

}
