package org.hfgiii.sesportfolio.analytics.client.esexamples

import java.text.SimpleDateFormat
import java.util.Date

import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats
import org.elasticsearch.search.sort.SortOrder
import org.hfgiii.sesportfolio.analytics._
import org.hfgiii.sesportfolio.analytics.model._
import org.hfgiii.ses.common.macros.SesMacros._
import org.hfgiii.ses.common.SesCommon._

import scala.collection.JavaConversions._

object EsExamples {

  def main(args:Array[String]) {

    implicit val client = initElasticsearch

   val rMap = Map("msft_ror" -> 0.2, "snp_ror" -> 0.1)

    val rrMap:Map[String,Map[String,Any]] = Map("ror1" -> Map("msft_ror" -> 0.2, "snp_ror" -> 0.1), "ror2" -> Map("msft_ror" -> 0.1, "snp_ror" -> 0.2))

    val rrrMap = Map("ror11" -> rrMap,"ror22" -> rrMap)
   // lazy val r      = RoRs()
    //val rCC   = fromMap[RoRs](rMap)
    //val rrCC  = fromMap[RoRSquared](rrMap)
    val rrrCC = fromMap[RoRCubed](rrrMap,(str:String) => str)


    val _rrrMap = toMap[RoRCubed](rrrCC,(str:String) => str)
   // val _rrMap = toMap[RoRSquared](rrCC)

    val _allEq =
      client.execute {
        count from "sesportfolio" -> "equity" where "name" -> "amzn"
      }.await.getCount

    println(s"amzn index size is ${_allEq}")

    val _allCls =
      client.execute {
        count from "daily_returns" -> "daily_ror"
      }.await.getCount

    println(s"daily_ror index size is ${_allEq}")

    val _namedAgg =
      client.execute {
        search in "sesportfolio" types "equity" aggs {
          aggregation terms "eqs" field "name" aggs {
            aggregation extendedstats "ror_stats" field "rate_of_return" script "doc['rate_of_return'].value" lang "groovy"
          }
        }
      }.await


    val _agnMap =
      _namedAgg.getAggregations.asMap().get("eqs").asInstanceOf[StringTerms].
        getBucketByKey("amzn").getAggregations.asMap.get("ror_stats").asInstanceOf[InternalExtendedStats].getStdDeviation

    println("aggMap")

    val x= 0


//    val _blib =
//      client.execute {
//        search in "sesportfolio" types "equity" aggs {
//         // aggregation terms "eqs" field "name" aggs {
//            aggregation extendedstats "ror_stats" field "rate_of_return" script "portfolioscript" lang "native"// params Map("fieldName"-> "rate_of_return")
//         // }
//        }
//      }.await
//
//
//    val blop =
//        client.execute {
//          search in "sesportfolio" types "positions" query matchall size 256 sort {
//            by field "date" order SortOrder.ASC
//          } scriptfields (
//               "balance" script "portfolioscript" lang "native" params Map("fieldName"-> "rate_of_return"),
//               "date"    script "doc['date'].value" lang "groovy"
//            )
//        }.await

//        println(s"BLOP total hits = ${blop.getHits}")


    val gort =
    search in "sesportfolio" types "positions" query matchall size 256 sort {
      by field "date" order SortOrder.ASC
    } scriptfields(
       script field "balance" script "portfolioscript" lang "native" params Map("fieldName" -> "rate_of_return"),
       script field "date" script "doc['date'].value" lang "groovy"
      )

    println(s"GORT: ${gort._builder.toString  }") //

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
    }


     aggActionFromSearchResponse {
      client.execute {
        search in "simulation" types "ror" aggs {
          aggregation extendedstats "s_ror_stats" field "rate_of_return" script "doc['rate_of_return'].value"
        }
      }
    } (aggName = "s_ror_stats") {
      case _exStats : InternalExtendedStats =>
        println(s"_exsStats avg = ${_exStats.getAvg}, std dev = ${_exStats.getStdDeviation}")
    }


    val res =
      client.execute {
        search in "sesportfolio" -> "equity" query {
          bool {
            must (termQuery("date", "2011-01-03"))
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


    aggActionFromSearchResponse {
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



    val psjson =
    (search in "products" types "product" query {
      filteredQuery filter {
        termFilter("scode", "1240069")
      }
    } fields("code","dtype","level","scode"))._builder.toString

    println(s"PSJSON: $psjson")

    val ps =
    client.execute {
      search in "products" types "product" query {
        filteredQuery filter {
          termFilter("scode", "1240069")
        }
      } fields("code","dtype","level","scode")
    }.await

    val pjsonstr =
      (search in "products" types "product" aggs {
        aggregation terms "scodes" field "scode" aggs {
          aggregation terms "codes" field "code"
        }
      })._builder.toString



    val pss =
      client.execute {
        search in "products" types "product" aggs {
          aggregation terms "scodes" field "scode" aggs {
            aggregation terms "codes" field "code"
          }

        }
      }.await

    aggActionFromBucket {
      bucketFromSearchResponse (pss) ("groups") {
        case terms: StringTerms =>
          val bucket = terms.getBucketByKey("1240069")
          println(s"Top Buck ${bucket.getKey} count = ${bucket.getDocCount}")
          Option(terms.getBucketByKey("1240069"))
      }
    }("nug") {
      case x :StringTerms => {
        x.getBuckets.map {
          bucket =>
            println(s"bucket key = ${bucket.getKey}")

        }
        //println(s"x = $x")
      }
    }

    val hflds =
    hitsFromSearchResponse(pss) {
      case hits:SearchHits => Option(hits.getHits.map {
        _.sourceAsString()
      })

    }
    val xxxx = 0

    val shdown =
      client.shutdown.await

    println(shdown)


  }

}
