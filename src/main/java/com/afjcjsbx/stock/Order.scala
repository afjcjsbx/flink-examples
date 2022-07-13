package com.afjcjsbx.stock

case class Order(parentOrderId: Int,
                 action: Action,
                 quantity: Int,
                 limitPrice: Double,
                 takeProfitLimitPrice: Double,
                 stopLossPrice: Double)
