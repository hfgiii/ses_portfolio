package org.hfgiii.sesportfolio.analytics

import com.novus.salat.annotations.raw.Salat
import com.sksamuel.elastic4s.IndexDefinition

package object model {

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
  case class EquityOrder(date:String,symbol:String="",ordertype:OrderType=UnknownOrderType,volume:Long=0l)

  case class Position(symbol:String,volume:Long=0l,value:Double=0d)

  case class UAProduct(code:String,dtype:String,level:Int,scode:String)



  case class Optimized(msft:Double=0d,amzn:Double=0d,ebay:Double=0d,ups:Double=0d,sr:Double=0d)
  case class Opts(sharperatio:Boolean = false,optimize:Boolean = false,equity:String="all",beta:Boolean=false,init:Boolean=false,xit:Boolean=false,tradesim:Boolean=false)

  case class RoRListAccumulator(lastClose:Double,rors:List[Double] = List.empty[Double])

  case class RoRSimpleIndexAccumulator(lastClose:Double,
                                       rorIndexDefinitions:List[IndexDefinition] = List.empty[IndexDefinition])


  case class RoRs(msft_ror:Double =0d,snp_ror:Double=0d)
  case class RoRSquared(ror1:RoRs,ror2:RoRs)
  case class RoRCubed(ror11:RoRSquared,ror22:RoRSquared)


  case class RoRWithSnPIndexAccumulator(lastCloses:(Double,Double) = (0d,0d),
                                        rorIndexDefinitions:List[IndexDefinition] = List.empty[IndexDefinition])

  case class EquityPriceIndexAccumulator(lastEquity:String = "",
                                         lastClose:Double = 0d,
                                         equityIndex:List[IndexDefinition] = List.empty[IndexDefinition],
                                         closingIndex:List[IndexDefinition] = List.empty[IndexDefinition],
                                         rorIndex:Map[String,IndexDefinition] = Map.empty[String,IndexDefinition])

  case class PortfolioPositions(date:Option[String],positions:List[Position])


  case class LinearRegressionArgs(domain:List[(Double,Double)] = List.empty[(Double,Double)],codomain:List[Double] = List.empty[Double])


  case class PortfolioBalance(date:Long,balance:Double)

}
