package org.hfgiii.sesportfolio

import java.io.File
import java.util.UUID

import com.sksamuel.elastic4s.{IndexDefinition, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.search.aggregations.Aggregation
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats
import org.hfgiii.sesportfolio.model.EquityPerformance
import org.hfgiii.sesportfolio.parser.{CSVParserIETFAction, CSVParboiledParserSB}
import org.parboiled2.{ParseError, ParserInput}

import scala.concurrent.Future
import scala.util.{Failure, Success}


package object analytics {

  case class Optimized(msft:Double=0d,amzn:Double=0d,ebay:Double=0d,ups:Double=0d,sr:Double=0d)
  case class Opts(sharperatio:Boolean = false,optimize:Boolean = false,equity:String="all",init:Boolean=false)
  case class IndexAccumulator(lastClose:Double = 0d,
                              equityIndex:List[IndexDefinition] = List.empty[IndexDefinition],
                              closingIndex:Map[String,IndexDefinition] = Map.empty[String,IndexDefinition])


  case class EquityPriceParser(name:String,input: ParserInput) extends CSVParboiledParserSB with CSVParserIETFAction {

    def parseEquities:List[EquityPerformance] =
      csvfile.run() match {
        case Success(result) => result.asInstanceOf[List[EquityPerformance]]

        case Failure(e: ParseError) => println("Expression is not valid: " + formatError(e)) ; List.empty[EquityPerformance]
        case Failure(e) => println("Unexpected error during parsing run: " + e) ; List.empty[EquityPerformance]
      }
  }


  val equities = List("msft","amzn","ebay","ups")

  def initElasticsearch:ElasticClient = {

    val tempFile = File.createTempFile("elasticsearchtests", "tmp")
    val homeDir = new File(tempFile.getParent + "/" + UUID.randomUUID().toString)
    homeDir.mkdir()
    homeDir.deleteOnExit()
    tempFile.deleteOnExit()

    val settings = ImmutableSettings.settingsBuilder()
      .put("node.http.enabled", false)
      .put("http.enabled", false)
      .put("path.home", homeDir.getAbsolutePath)
      .put("index.number_of_shards", 1)
      .put("index.number_of_replicas", 0)
      .put("script.disable_dynamic", false)
      .put("es.logger.level", "INFO")

    val client =
      ElasticClient.local(settings.build)

    loadIndex(client)

    client
  }

  def shutdownElasticsearch(implicit client:ElasticClient) {
      client.shutdown.await
  }

  def emptyClient:ElasticClient = ElasticClient.local

  def loadIndex(implicit client:ElasticClient): Unit = {
    val createIdxResponse =
      client.execute {
        create index "sesportfolio" mappings (
          "equity" as(
            "name" typed StringType,
            "date" typed DateType,
            "open" typed DoubleType,
            "high" typed DoubleType,
            "low" typed DoubleType,
            "close" typed DoubleType,
            "volume" typed LongType,
            "adj_close" typed DoubleType,
            "rate_of_return" typed DoubleType
            )
          )
      }.await


    val createPortIdxResponse =
      client.execute {
        create index "portfolio" mappings {
          "closing" as (
            "date" typed DateType,
            "msft_close" typed DoubleType,
            "amzn_close" typed DoubleType,
            "ebay_close" typed DoubleType,
            "ups_close"  typed DoubleType

            )
        }
      }.await

    println(createIdxResponse.writeTo(new OutputStreamStreamOutput(System.out)))


    val equityPrices =

      equities.foldLeft(List.empty[EquityPerformance]) {
        (lst,equity) =>
          val finput = this.getClass.getResourceAsStream(s"/${equity}_11.csv")

          val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

          lst ++ EquityPriceParser(equity,inputfile).parseEquities

      }


    val idxAccumulator =
      equityPrices.foldLeft(IndexAccumulator()) {
        (ip,sp) =>

          val rate_of_return =
            if(ip.lastClose == 0d) 0d
            else  (sp.adj_close / ip.lastClose) - 1

          val equityIndex = index into "sesportfolio/equity" fields (
            "name" -> sp.name,
            "date" -> sp.date,
            "open" -> sp.open,
            "high" -> sp.high,
            "low"  -> sp.low,
            "close" -> sp.close,
            "volume" -> sp.volume,
            "adj_close" -> sp.adj_close,
            "rate_of_return" -> rate_of_return
            )

          val closingIndex =
            ip.closingIndex.get(sp.date) match {
              case Some(pidx) => sp.date -> {
                pidx.fields (s"${sp.name}_close" -> rate_of_return)
                pidx
              }
              case None       => sp.date -> {
                index into "portfolio/closing" fields (
                  "date" -> sp.date,
                  s"${sp.name}_close" -> rate_of_return
                  )
              }
            }

          IndexAccumulator(lastClose = sp.adj_close,
            equityIndex = equityIndex :: ip.equityIndex,
            closingIndex = ip.closingIndex + closingIndex)
      }


    val eqIndexed = idxAccumulator.equityIndex.toSeq

    client execute {
      bulk(eqIndexed: _ *)
    }

    blockUntilCount(eqIndexed.length,"sesportfolio")

    val clsIndexed = idxAccumulator.closingIndex.values.toSeq

    client execute {
      bulk(clsIndexed: _ *)
    }

    blockUntilCount(clsIndexed.length,"portfolio")
  }

  def aggActionFromSearchResponse(sp:SearchResponse)(aggName:String)(action :PartialFunction[Aggregation,Unit]) {
    Option(sp.getAggregations.asMap.get(aggName)).fold (println(s"No $aggName")) {
      action orElse {
        case x:Aggregation =>
          println(s"Unexpected statistics returned: ${x.getClass.getCanonicalName}")
      }
    }
  }

  def aggActionFromBucket(bucket:Option[Bucket])(aggName:String)(action :PartialFunction[Aggregation,Unit]) =
    bucket.fold(println(s"No $aggName")) { sp =>
      Option(sp.getAggregations.asMap.get(aggName)).fold(println(s"No $aggName")) {
        action orElse {
          case x: Aggregation =>
            println(s"Unexpected statistics returned: ${x.getClass.getCanonicalName}")
        }
      }
    }



  def bucketFromSearchResponseFuture(sp:Future[SearchResponse])(aggName:String)(action :PartialFunction[Aggregation,Option[Bucket]]):Option[Bucket] =
    Option(sp.await.getAggregations.asMap.get(aggName)) match {
      case Some(agg) => action.applyOrElse (agg, (x:Aggregation) => {
        println(s"Unexpected statistics returned: ${x.getClass.getCanonicalName}"); None

      })

      case None => println(s"No $aggName") ; None
    }

  def bucketFromSearchResponse(sp:SearchResponse)(aggName:String)(action :PartialFunction[Aggregation,Option[Bucket]]):Option[Bucket] =
    Option(sp.getAggregations.asMap.get(aggName)) match {
      case Some(agg) => action.applyOrElse (agg, (x:Aggregation) => {
        println(s"Unexpected statistics returned: ${x.getClass.getCanonicalName}"); None

      })

      case None => println(s"No $aggName") ; None
    }

  def aggActionFromSearchResponseFuture[R](sp:Future[SearchResponse])(aggName:String)(action :PartialFunction[Aggregation,Unit]) {
    Option(sp.await.getAggregations.asMap.get(aggName)).fold (println(s"No $aggName")) {
      action orElse {
        case x:Aggregation =>
          println(s"Unexpected statistics returned: ${x.getClass.getCanonicalName}")

      }
    }
  }

  def aggFromSearchResponseFuture[R](sp:Future[SearchResponse])(aggName:String)(action :PartialFunction[Aggregation,Option[R]]):Option[R] = {
    Option(sp.await.getAggregations.asMap.get(aggName)) match {
      case Some(agg) => action.applyOrElse (agg, (x:Aggregation) => {
        println(s"Unexpected statistics returned: ${x.getClass.getCanonicalName}"); None

      })

      case None => println(s"No $aggName") ; None
    }
  }

  def blockUntilCount(expected: Long,
                      index: String,
                      types: String*)(implicit client:ElasticClient) {

    var backoff = 0
    var actual = 0l

    while (backoff <= 50 && actual != expected) {
      if (backoff > 0)
        Thread.sleep(100)
      backoff = backoff + 1
      try {
        actual = client.execute {
          count from index types types
        }.await.getCount
      } catch {
        case e: IndexMissingException => 0
      }
    }
    println(s"actual is $actual")
    require(expected == actual, s"Block failed waiting on count: Expected was $expected but actual was $actual")
  }

  def refresh(indexes: String*)(implicit client:ElasticClient) {
    val i = indexes.size match {
      case 0 => Seq("_all")
      case _ => indexes
    }
    val listener = client.client.admin().indices().prepareRefresh(i: _*).execute()
    listener.actionGet()
  }



  def weightedSharpe(m_per:Int,a_per:Int,e_per:Int,u_per:Int)(implicit client:ElasticClient):Option[Optimized] = {

    val m_bal = 0.1 * m_per
    val a_bal = 0.1 * a_per
    val e_bal = 0.1 * e_per
    val u_bal = 0.1 * u_per

    val balanceScript =
      s"$m_bal*doc['msft_close'].value + $a_bal*doc['amzn_close'].value + $e_bal*doc['ebay_close'].value + $u_bal*doc['ups_close'].value"

    aggFromSearchResponseFuture[Optimized] {
      client.execute {
        search in "portfolio" types "closing" aggs {
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

    equities.map { equity =>
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
      bucketFromSearchResponseFuture {
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
