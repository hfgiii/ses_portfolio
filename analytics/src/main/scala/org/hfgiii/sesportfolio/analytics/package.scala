package org.hfgiii.sesportfolio

import java.io.File
import java.util.UUID

import breeze.linalg.{DenseMatrix, DenseVector}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._
import com.sksamuel.elastic4s.{ElasticClient, IndexDefinition}
import nak.regress.LinearRegression
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.Aggregation
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats
import org.elasticsearch.search.sort.SortOrder
import org.hfgiii.sesportfolio.model.EquityPerformance
import org.hfgiii.sesportfolio.parser.{CSVParboiledParserSB, CSVParserIETFAction}
import org.parboiled2.{ParseError, ParserInput}
import shapeless._
import poly._
import syntax.std.tuple._

import scala.concurrent.Future
import scala.util.{Failure, Success}


package object analytics {

  case class Optimized(msft:Double=0d,amzn:Double=0d,ebay:Double=0d,ups:Double=0d,sr:Double=0d)
  case class Opts(sharperatio:Boolean = false,optimize:Boolean = false,equity:String="all",beta:Boolean=false,init:Boolean=false,xit:Boolean=false)

  case class RoRIndexAccumulator(lastCloses:(Double,Double) = (0d,0d),
                                 rorIndexDefinitions:List[IndexDefinition] = List.empty[IndexDefinition])

  case class IndexAccumulator(lastEquity:String = "",
                              lastClose:Double = 0d,
                              equityIndex:List[IndexDefinition] = List.empty[IndexDefinition],
                              closingIndex:Map[String,IndexDefinition] = Map.empty[String,IndexDefinition])


  case class LinearRegressionArgs(domain:List[(Double,Double)] = List.empty[(Double,Double)],codomain:List[Double] = List.empty[Double])


  case class EquityPriceParser(name:String,input: ParserInput) extends CSVParboiledParserSB with CSVParserIETFAction {

    def parseEquities:List[EquityPerformance] =
      csvfile.run() match {
        case Success(result) => result.asInstanceOf[List[EquityPerformance]]

        case Failure(e: ParseError) => println("Expression is not valid: " + formatError(e)) ; List.empty[EquityPerformance]
        case Failure(e) => println("Unexpected error during parsing run: " + e) ; List.empty[EquityPerformance]
      }
  }


  val equitiesForDailies = List("msft","amzn","ebay","ups","snp")
  val equitiesForWeeklies = List("msft","snp")

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

    loadIndexes(client)

    client
  }

  def shutdownElasticsearch(implicit client:ElasticClient) {
      client.shutdown.await
  }

  def emptyClient:ElasticClient = ElasticClient.local
//
  def ror(ip:IndexAccumulator,sp:EquityPerformance):Double =
    if(ip.lastClose == 0d || ip.lastEquity.compare(sp.name) != 0) 0d
    else  (sp.adj_close / ip.lastClose) - 1

  def newRoRIndex(indexNType:String)(ip:IndexAccumulator,sp:EquityPerformance,rate_of_return:Double):(String,IndexDefinition) =
    ip.closingIndex.get(sp.date) match {
      case Some(pidx) => sp.date -> {
        pidx.fields(s"${sp.name}_ror" -> rate_of_return)
        pidx
      }
      case None => sp.date -> {
        index into indexNType fields(
          "date" -> sp.date,
          s"${sp.name}_ror" -> rate_of_return
          )
      }
    }


  val newDailyRoRIndex = newRoRIndex("daily_returns/daily_ror") _


  def rorSimple(lastClose:Double,adjClose:Double):Double =
    if(lastClose == 0d) 0d
    else  (adjClose / lastClose) - 1

  object _ror extends ((Double,Double) -> Double) ((rorSimple _).tupled)

  def loadWeeklyPrices(implicit client:ElasticClient) {

    val weeklyEquityPrices =
      equitiesForWeeklies.foldLeft(Map.empty[String,List[EquityPerformance]]) {
        (lst,equity) =>
          val finput = this.getClass.getResourceAsStream(s"/${equity}_week.csv")

          val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

          lst + (equity ->  EquityPriceParser(equity,inputfile).parseEquities)

      }

    val weekZip = weeklyEquityPrices("msft").zip(weeklyEquityPrices("snp"))

    val idxAccumulator =
      weekZip.foldLeft(RoRIndexAccumulator()) {
        (idx,tpl) =>
        val (msft,snp) = tpl

          val rors =
            (((idx.lastCloses._1,msft.adj_close),(idx.lastCloses._2,snp.adj_close)) map _ror).asInstanceOf[(Double,Double)]

          val idxDefinition =
            index into "weekly_returns/weekly_ror" fields(
              "date" -> msft.date,
              "msft_ror" -> rors._1,
              "snp_ror"  -> rors._2)

          RoRIndexAccumulator(lastCloses = (msft.adj_close,snp.adj_close),
             rorIndexDefinitions = idxDefinition :: idx.rorIndexDefinitions)
        }

    val clsIndexed =
      idxAccumulator.rorIndexDefinitions.toSeq

    client execute {
      bulk(clsIndexed: _ *)
    }

    blockUntilCount(clsIndexed.length,"weekly_returns")

  }
  
  def loadDailyPrices(implicit client:ElasticClient) {

    val dailyEquityPrices =

      equitiesForDailies.foldLeft(List.empty[EquityPerformance]) {
        (lst,equity) =>
          val finput = this.getClass.getResourceAsStream(s"/${equity}_11.csv")

          val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

          lst ++ EquityPriceParser(equity,inputfile).parseEquities

      }

    val idxAccumulator =
      dailyEquityPrices.foldLeft(IndexAccumulator()) {
        (ip,sp) =>

          val rate_of_return = ror(ip,sp)

//          if(sp.name.compare("ebay") == 0)
//          println(s"ROR::::Name:${sp.name},Date:${sp.date},ROR:$rate_of_return")

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

          IndexAccumulator(
            lastEquity = sp.name,
            lastClose = sp.adj_close,
            equityIndex = equityIndex :: ip.equityIndex,
            closingIndex = ip.closingIndex + newDailyRoRIndex(ip,sp,rate_of_return))
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

    blockUntilCount(clsIndexed.length,"daily_returns")
  }

  def loadIndexes(implicit client:ElasticClient): Unit = {
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
        create index "daily_returns" mappings (
          "daily_ror" as (
            "date" typed DateType,
            "msft_ror" typed DoubleType,
            "amzn_ror" typed DoubleType,
            "ebay_ror" typed DoubleType,
            "upd_ror"  typed DoubleType,
            "snp_ror"  typed DoubleType

            )
          )
      }.await

    val createWeeklyReturnsResponse =
      client.execute {
        create index "weekly_returns" mappings (
          "weekly_ror" as (
            "date" typed DateType,
            "msft_ror" typed DoubleType,
            "snp_ror"  typed DoubleType
            )
          )
      }.await


   // println(createIdxResponse.writeTo(new OutputStreamStreamOutput(System.out)))

    loadDailyPrices

    loadWeeklyPrices

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

  def hitsFromSearchResponseFuture[R](sp:Future[SearchResponse])(action :PartialFunction[SearchHits,Option[R]]):Option[R] =
    Option(sp.await.getHits) match {
      case Some(hits) => action.applyOrElse(hits, (x:SearchHits) => {
        println(s"Unexpected statistics returned: ${x.getClass.getCanonicalName}"); None

      })

      case None => println(s"No Search Hits!") ; None

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
      s"$m_bal*doc['msft_ror'].value + $a_bal*doc['amzn_ror'].value + $e_bal*doc['ebay_ror'].value + $u_bal*doc['upd_ror'].value"

    aggFromSearchResponseFuture[Optimized] {
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
  
  def linearRegression(domain:DenseMatrix[Double],codomain:DenseVector[Double]): Unit = {
    {
//      val features = DenseMatrix.create[Double](3, 3, Array(0, 0, 0, 0.9, 2.5, 3.0, 1.6, 4.0, 5.7)).t
//      val target = DenseVector[Double](1.0, 2.0, 3.0)
//      val result = LinearRegression.regress(features, target)
//      val data = DenseMatrix((60.0,1.0),(61.0, 1.0), (62.0, 1.0),(63.0, 1.0),(65.0, 1.0))
//      val y = DenseVector(60.0,61.0,62.0,63.0,65.0) // DenseVector(3.1,3.6,3.8,4.0,4.1)
      val beta = LinearRegression.regress(domain,codomain)
      println( s"Linear Regression Result $beta")
    }
  }


  def betaCalculationForMSFT(implicit client:ElasticClient) {
    val lrargs =
    hitsFromSearchResponseFuture {
      client.execute {
        search in "weekly_returns" types "weekly_ror" fields("msft_ror", "snp_ror") size 197 query matchall sort {
          by field "date" order SortOrder.DESC
        }
      }
     } {
        case hits:SearchHits => Option(hits.getHits.foldLeft(LinearRegressionArgs()) {
          (lr,hit) =>
            val eqRet = hit.getFields.get("msft_ror").getValue.asInstanceOf[Double]
            val idxRet = hit.getFields.get("snp_ror").getValue.asInstanceOf[Double]

            LinearRegressionArgs((idxRet,1.0) :: lr.domain,eqRet :: lr.codomain)
        })
      }


    val deqs = lrargs.fold(Seq.empty[(Double,Double)]) {
      _.domain.toSeq
    }

    val codeqs = lrargs.fold(Seq.empty[Double]) {
      _.codomain.toSeq
    }

    linearRegression(DenseMatrix(deqs: _*),DenseVector(codeqs: _*))
  }

}
