package com.github.nlzimmerman

object DirectQuantile {
  def getQuantiles(x: Seq[Double], quantiles: Seq[Double]): Seq[Double] = {
    val s: Seq[Double] = x.sorted
    val l: Long = s.length
    val targetRanks: Seq[Int] = quantiles.map(
      // math.ceil((seq.length - 1) * (p / 100.0)).toInt
      (x) => Math.ceil((l-1)*x).toInt
    )
    targetRanks.map(s(_))
  }

}
