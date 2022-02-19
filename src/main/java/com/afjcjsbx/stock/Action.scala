package com.afjcjsbx.stock


sealed trait Action

case object Buy extends Action
case object Sell extends Action