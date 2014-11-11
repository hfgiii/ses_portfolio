package org.hfgiii.sesportfolio.analytics

import java.text.SimpleDateFormat
import java.util.Date

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats
import org.elasticsearch.search.sort.SortOrder
import org.hfgiii.ses.common.macros.SesMacros._
import org.hfgiii.ses.common.SesCommon._
import org.hfgiii.sesportfolio.analytics.model._
import org.parboiled2.ParserInput


package object simulation {

  def groupPricesByDate(equities:List[String]):Map[String,Map[String,EquityPrices]] = {
    val rm =
      equities.foldLeft(Map.empty[String,Map[String,EquityPrices]]) {
        (m0,equity) =>
          val finput = this.getClass.getResourceAsStream(s"/${equity}_11.csv")

          val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString
          val m1 =
            EquityPriceParser(equity,inputfile).
              parseEquities.foldLeft(m0) {
              (m,ep) =>
                m.get(ep.date).fold {
                  m + (ep.date -> Map(ep.name -> ep))
                } {
                  ms =>
                    m + (ep.date -> (ms + (ep.name -> ep)))
                }
            }
          m1
      }
    rm
  }


  def groupOrdersByDate(orderPath:String):Map[String,List[EquityOrder]] = {
    val finput = this.getClass.getResourceAsStream("/allorders.csv")

    val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

    EquityOrderParser(input = inputfile).
      parseEquityOrders.foldRight(Map.empty[String,List[EquityOrder]]) {
      (eo,m) =>
        m.get(eo.date).fold {
          m + (eo.date -> (eo :: Nil))
        } {
          meo => m +  (eo.date -> (eo :: meo))
        }

    }

  }



  val initialPosition = PortfolioPositions(date = None,List(Position(symbol = "cash",volume = 1000000,value = 10000000d)))

  def newPosition(currentDate:String,eqOrders:List[EquityOrder],prices:Map[String,EquityPrices],lastPosition:Option[PortfolioPositions] = None):PortfolioPositions = {

    def calcPositionNoOrders(currentDate:String,prices:Map[String,EquityPrices],lastPosition:PortfolioPositions):PortfolioPositions = {
      val newposes = lastPosition.positions.map {
        pos => Position(symbol = pos.symbol,
          volume = pos.volume,
          value  = pos.volume * prices.get(pos.symbol).fold(1.0)(_.adj_close)) //1.0 is for CASH
      }

      PortfolioPositions(date = Option(currentDate), newposes)

    }
    def calcPositionWithOrders(currentDate:String,eqOrders:List[EquityOrder],prices:Map[String,EquityPrices],lastPosition:PortfolioPositions):PortfolioPositions = {

      val (cash, eqs) = lastPosition.positions.partition(_.symbol == "cash")
      val gbOrders = eqOrders.groupBy(_.symbol)

      val newPoses =
        eqs match {
          case Nil => gbOrders.keys.toList.foldLeft(List.empty[Position]) {
            (list,key) =>
              prices.get(key).fold {
                list
              }{
                ep =>
                  Position(symbol =key,
                    volume = gbOrders(key).head.volume,
                    value  = gbOrders(key).head.volume * ep.adj_close) :: list
              }
          }

          case _   => eqs.foldLeft(List.empty[Position]) {
            (list, pos) =>
              gbOrders.get(pos.symbol).fold {
                Position(symbol = pos.symbol,
                  volume = pos.volume,
                  value = pos.volume * prices.get(pos.symbol).fold(1.0)(_.adj_close)) :: list
              } {
                eo =>
                  eo.head.ordertype match {
                    case BuyToOpen => {
                      val value = eo.head.volume * prices.get(pos.symbol).fold(1.0)(_.adj_close)

                      val eqPos =
                        Position(symbol = pos.symbol,
                          volume = eo.head.volume,
                          value = value)

                      val cashPos =
                        Position(symbol = "cash",
                          volume = -value.toInt,
                          value = -value)

                      eqPos :: cashPos :: list
                    }

                    case SellToClose => {

                      val value = eo.head.volume * prices.get(pos.symbol).fold(1.0)(_.adj_close)

                      Position(symbol = "cash",
                        volume = value.toInt,
                        value = value) :: list
                    }
                  }
              }
          }
        }


      val (newCashPos,newEqPos) = newPoses.partition(_.symbol == "cash")

      val newCashPosition =
        newCashPos.foldLeft(cash.head) {
          (oldCash,newCash) =>
            Position(symbol = "cash",
              volume = oldCash.volume + newCash.volume,
              value  = oldCash.volume + newCash.volume
            )
        }
      PortfolioPositions(date = Option(currentDate),positions = newCashPosition :: newEqPos)
    }

    eqOrders match {
      //No Orders for this Day
      case Nil => {
        lastPosition.fold {
          // No Prior Orders - Initial State
          initialPosition
        } {
          //Prior Order Exists
          lp => calcPositionNoOrders(currentDate,prices,lp)
        }
      }

      case eorders => {
        lastPosition.fold {
          // No Prior Orders - Initial State
          calcPositionWithOrders(currentDate,eorders,prices,initialPosition)
        } {
          //Prior Order Exists
          lp =>
            calcPositionWithOrders(currentDate,eorders,prices,lp)
        }
      }
    }
  }

  def updatePortfolio(date:String,eqOrders:List[EquityOrder],prices:Map[String,EquityPrices],oldPositions:List[PortfolioPositions]):List[PortfolioPositions] =
    oldPositions match {
      case x if x.tail == Nil => x.head.date.fold {
        newPosition(date,eqOrders,prices) :: Nil
      } {
        _ => {
          newPosition(date,eqOrders,prices,Some(x.head)) :: oldPositions
        }
      }

      case x => {
        newPosition(date,eqOrders,prices,Some(x.head)) :: oldPositions
      }
    }



  def tradeSimulation(implicit client:ElasticClient) {
    val rors =
      hitsFromSearchResponse {
        client.execute {
          search in "sesportfolio" types "positions" query matchall size 256 sort {
            by field "date" order SortOrder.ASC
          } scriptfields(
            script field "balance" script "portfolioscript" lang "native" params Map("fieldName" -> "rate_of_return"),
            script field "date" script "doc['date'].value" lang "groovy"
            )
        }
      }{
        case hits:SearchHits =>
          val formatter = new SimpleDateFormat("yyyy-MM-dd")

          Option (hits.getHits.foldLeft(RoRSimpleIndexAccumulator(lastClose = 1000000d)) {
            (ror,hit) =>

              val PortfolioBalance(date,balance) = fromHitFields[PortfolioBalance](hit)
              val ts =  new Date(date)

              println(s"returned date $ts")
              println(s"last balance ${ror.lastClose}")
              println(s"returned balance $balance")

              val rorCalc = if(ror.lastClose == 0d || ror.lastClose == balance) 0d
              else (balance / ror.lastClose) - 1

              println(s"RATE OF RETURN = $rorCalc")

              val idxDef = index into "simulation/ror" fields (
                "date" -> formatter.format(ts),
                "rate_of_return" -> rorCalc
                )

              RoRSimpleIndexAccumulator(balance, idxDef :: ror.rorIndexDefinitions)
          })
      }

    rors.fold(println("NO RETURNS")) {
      rors =>

        bulkIndexLoad("simulation",rors.rorIndexDefinitions.toSeq,rors.rorIndexDefinitions.length)

        aggActionFromSearchResponse {
          client.execute {
            search in "simulation" types "ror" sort {
              by field "date" order SortOrder.ASC
            } aggs {
              aggregation extendedstats "s_ror_stats" field "rate_of_return" script "doc['rate_of_return'].value"
            }
          }
        } (aggName = "s_ror_stats") {
          case _exStats : InternalExtendedStats =>
            println(s"_exsStats avg = ${_exStats.getAvg}, std dev = ${_exStats.getStdDeviation}")
        }
    }


  }


}
