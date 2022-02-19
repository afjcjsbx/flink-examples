package com.afjcjsbx.stock

case object DummySignal extends Signal {
    override def entry(prices: Seq[Double]): Option[Order] = {
        if (prices.size > 1) {
            if (prices.head - prices(1) > 3) Order(1000, Buy, 1, 1, 1, .5)
            if (prices.head - prices(1) < -3) Order(1000, Buy, 1, 1, 1, .5)
        }
    }
}
