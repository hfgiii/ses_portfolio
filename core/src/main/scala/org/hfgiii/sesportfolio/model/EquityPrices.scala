package org.hfgiii.sesportfolio.model

import com.novus.salat.annotations.raw.Salat

case class EquityPrices(name:String,date:String,open:Double,high:Double,low:Double,close:Double,volume:Long,adj_close:Double)

@Salat
trait OrderType {
  def name:String
}
case object BuyToOpen extends OrderType {
  def name = "Buy"
}
case object SellToClose extends OrderType {
  def name = "Sell"
}
case object UnknownOrderType extends OrderType {
  def name = "Unknown"
}
case class EquityOrder(date:String,symbol:String,ordertype:OrderType,volume:Long)
