package org.hfgiii.sesportfolio.analytics

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats
import org.hfgiii.ses.common.SesCommon._
import org.hfgiii.sesportfolio.analytics.model.Optimized

package object sharperatio {

  def weightedSharpe(m_per:Int,a_per:Int,e_per:Int,u_per:Int)(implicit client:ElasticClient):Option[Optimized] = {

  val m_bal = 0.1 * m_per
  val a_bal = 0.1 * a_per
  val e_bal = 0.1 * e_per
  val u_bal = 0.1 * u_per

  val balanceScript =
    s"$m_bal*doc['msft_ror'].value + $a_bal*doc['amzn_ror'].value + $e_bal*doc['ebay_ror'].value + $u_bal*doc['upd_ror'].value"

  aggFromSearchResponse[Optimized] {
    client.execute {
      search in "daily_returns" types "daily_ror" aggs {
        aggregation extendedstats "ror_stats" script balanceScript
      }
    }
  } ("ror_stats") {

    case _rrexStats : InternalExtendedStats => {

      val opt = Optimized(m_bal,a_bal,e_bal,u_bal,Math.sqrt(250) * _rrexStats.getAvg /_rrexStats.getStdDeviation)

      println (s"Optimized:$opt")

      Option(opt)

    }
  }

}

  def optimize(implicit client:ElasticClient) {
    val rats =
      for { msft <- 10 to 100 by 10
            amzn <- 10 to 100 by 10
            ebay <- 10 to 100 by 10
            ups <- 10 to 100 by 10
            if msft+amzn+ebay+ups == 100
            rat <- weightedSharpe(msft,amzn,ebay,ups)
      } yield rat

    val opt =
      rats.foldLeft(Optimized()) {
        (m,o) =>
          if(o.sr > m.sr) o
          else m
      }

    println(s"Number of Rats:${rats.length}")

    println (s"Max Sharp Ratio Optimized:$opt")


  }

  def allSharpeRatios(implicit client:ElasticClient){

    val bucketedRoR =
      client.execute {
        search in "sesportfolio" types "equity" aggs {
          aggregation terms "eqs" field "name" aggs {
            aggregation extendedstats "ror_stats" field "rate_of_return" script "doc['rate_of_return'].value"
          }
        }
      }.await

    equitiesForDailies.map { equity =>
      aggActionFromBucket {
        bucketFromSearchResponse(bucketedRoR)("eqs") {
          case terms: StringTerms =>
            Option(terms.getBucketByKey(equity))
        }
      }("ror_stats") {
        case _rrexStats: InternalExtendedStats => {

          val sr =
            Math.sqrt(250) * _rrexStats.getAvg / _rrexStats.getStdDeviation

          println(s"$equity ror_stats avg = ${_rrexStats.getAvg}, std dev = ${_rrexStats.getStdDeviation}, sharpe's ratio $sr")
        }
      }
    }
  }

  def singleSharpRatio(equity:String) (implicit client:ElasticClient): Unit = {
    aggActionFromBucket {
      bucketFromSearchResponse {
        client.execute {
          search in "sesportfolio" types "equity" aggs {
            aggregation terms "eqs" field "name" aggs {
              aggregation extendedstats "ror_stats" field "rate_of_return" script "doc['rate_of_return'].value"
            }
          }
        }
      }("eqs") {
        case terms: StringTerms =>
          Option(terms.getBucketByKey(equity))
      }
    }("ror_stats") {

      case _rrexStats: InternalExtendedStats => {

        val sr =
          Math.sqrt(250) * _rrexStats.getAvg / _rrexStats.getStdDeviation

        println(s"$equity ror_stats avg = ${_rrexStats.getAvg}, std dev = ${_rrexStats.getStdDeviation}, sharpe's ratio $sr")
      }
    }
  }

}
