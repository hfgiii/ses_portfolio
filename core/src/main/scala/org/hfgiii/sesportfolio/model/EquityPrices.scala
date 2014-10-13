package org.hfgiii.sesportfolio.model

case class EquityPrices(name:String,date:String,open:Double,high:Double,low:Double,close:Double,volume:Long,adj_close:Double)
trait OrderType {
  def name:String
}
case object BuyToOpen extends OrderType {
  def name = "BUY"
}
case object SellToClose extends OrderType {
  def name = "SELL"
}
case object UnknownOrderType extends OrderType {
  def name = "UNKNOWN"
}
case class EquityOrder(date:String,symbol:String,ordertype:OrderType,volume:Long)