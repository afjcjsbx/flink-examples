package com.afjcjsbx.stock

trait Signal {

    def entry(prices: Seq[Double]): Option[Order]

}
