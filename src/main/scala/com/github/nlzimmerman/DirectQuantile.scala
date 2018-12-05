package com.github.nlzimmerman

object DirectQuantile {
  def quantile(x: Seq[Double], quantiles: Seq[Double]): Seq[Double] = {
    val s: Seq[Double] = x.sorted
    val l: Long = s.length
    val targetRanks: Seq[Int] = quantiles.map(
      (x) => Math.round(x*l).toInt-1
    )
    targetRanks.map(s(_))
  }
}
