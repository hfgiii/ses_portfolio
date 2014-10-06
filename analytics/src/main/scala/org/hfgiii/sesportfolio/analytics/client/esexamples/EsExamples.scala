package org.hfgiii.sesportfolio.analytics.client.esexamples

import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats
import org.hfgiii.sesportfolio.analytics._


object EsExamples {

  def main(args:Array[String]) {

    implicit val client = initElasticsearch

    val _allEq =
      client.execute {
        count from "sesportfolio" -> "equity" where "name" -> "amzn"
      }.await.getCount

    println(s"index size is ${_allEq}")

    val _allCls =
      client.execute {
        count from "portfolio" -> "closing"
      }.await.getCount

    println(s"index size is ${_allEq}")

    val _namedAgg =
      client.execute {
        search in "sesportfolio" types "equity" aggs {
          aggregation terms "eqs" field "name" aggs {
            aggregation extendedstats "ror_stats" field "rate_of_return" script "doc['rate_of_return'].value"
          }
        }
      }.await

    val _agnMap =
      _namedAgg.getAggregations.asMap().get("eqs").asInstanceOf[StringTerms].
        getBucketByKey("amzn").getAggregations.asMap.get("ror_stats").asInstanceOf[InternalExtendedStats].getStdDeviation

    val res =
      client.execute {
        search in "sesportfolio" -> "equity" query {
          bool {
            must (term("date", "2011-01-03"))
          }
        }
      }.await

    // println(res.getHits.getAt(0).sourceAsMap())

    res.getHits.getHits.map {
      hit =>
        println(hit.getSourceAsString)
    }

    val _avg =
      client.execute {
        search in "sesportfolio" types "equity" aggs {
          aggregation avg "open_avg" field "open" script "doc['open'].value"
        }
      }.await


    aggActionFromSearchResponseFuture {
      client.execute {
        search in "sesportfolio" types "equity" aggs {
          aggregation extendedstats "open_stats" field "open" script "doc['open'].value"

        }
      }
    } (aggName = "open_stats") {
      case _exStats : InternalExtendedStats =>
        println(s"sopen_stats avg = ${_exStats.getAvg}, std dev = ${_exStats.getStdDeviation}")
    }


    val _stats =
      client.execute {
        search in "sesportfolio" types "equity" aggs {
          aggregation extendedstats "open_stats" field "open" script "doc['open'].value"

        }
      }.await

    val _rrstats =
      client.execute {
        search in "sesportfolio" types "equity" aggs {
          aggregation extendedstats "ror_stats" field "rate_of_return" script "doc['rate_of_return'].value"

        }
      }.await

    aggActionFromSearchResponse (_avg) ("open_avg") {
      case avgAgg : InternalAvg =>
        println(s"open_avg = ${avgAgg.getValue}")
    }


    aggActionFromSearchResponse (_stats) ("open_stats"){
      case _exStats : InternalExtendedStats =>
        println(s"open_stats avg = ${_exStats.getAvg}, std dev = ${_exStats.getStdDeviation}")
    }

    aggActionFromSearchResponse (_rrstats) ("ror_stats") {

      case _rrexStats : InternalExtendedStats => {

        val sr =
          Math.sqrt(250) * _rrexStats.getAvg /_rrexStats.getStdDeviation

        println(s"ror_stats avg = ${_rrexStats.getAvg}, std dev = ${_rrexStats.getStdDeviation}, sharpe's ratio $sr")

      }
    }

    val shdown =
      client.shutdown.await

    println(shdown)


  }

}
