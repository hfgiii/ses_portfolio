package org.hfgiii.sesportfolio

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import breeze.linalg.{DenseMatrix, DenseVector}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._
import com.sksamuel.elastic4s.{ElasticClient, IndexDefinition}
import nak.regress.LinearRegression
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats
import org.elasticsearch.search.sort.SortOrder
import org.hfgiii.ses.common.csv.parser.CSVParserIETFAction
import org.hfgiii.ses.common.macros.Mappable
import org.hfgiii.ses.common.dsl.response.readers.ResponseReaderDsl._
import org.hfgiii.sesportfolio.analytics.model._
import org.hfgiii.sesportfolio.analytics.csv.parser.CsvParsers._
import org.parboiled2.{ParseError, ParserInput}
import shapeless._
import poly._
import syntax.std.tuple._
import syntax.typeable._

import scala.util.{Failure, Success}


package object analytics {

  case class EquityPriceParser(name:String,input: ParserInput) extends CSVParboiledParserEquityPrice with CSVParserIETFAction {

    def parseEquities:List[EquityPrices] =
      csvfile.run() match {
        case Success(result) => result.cast[List[EquityPrices]].fold(List.empty[EquityPrices])(prices => prices)

        case Failure(e: ParseError) => println("Expression is not valid: " + formatError(e)) ; List.empty[EquityPrices]
        case Failure(e) => println("Unexpected error during parsing run: " + e) ; List.empty[EquityPrices]
      }
  }

  case class EquityOrderParser(input: ParserInput) extends CSVParboiledParserEquityOrder with CSVParserIETFAction {

    def parseEquityOrders:List[EquityOrder] =
      csvfile.run() match {
        case Success(result) => result.cast[List[EquityOrder]].fold(List.empty[EquityOrder])(orders => orders)

        case Failure(e: ParseError) => println("Expression is not valid: " + formatError(e)) ; List.empty[EquityOrder]
        case Failure(e) => println("Unexpected error during parsing run: " + e) ; List.empty[EquityOrder]
      }
  }

  case class UAProductParser(input: ParserInput) extends CSVParboiledParserUAProduct with CSVParserIETFAction {

    def parseUAProducts:List[UAProduct] =
      csvfile.run() match {
        case Success(result) => result.cast[List[UAProduct]].fold(List.empty[UAProduct])(orders => orders)

        case Failure(e: ParseError) => println("Expression is not valid: " + formatError(e)) ; List.empty[UAProduct]
        case Failure(e) => println("Unexpected error during parsing run: " + e) ; List.empty[UAProduct]
      }
  }

  val equitiesForDailies = List("aapl","ibm","msft","amzn","ebay","ups","xom","goog","snp")
  val equitiesForWeeklies = List("msft","snp")
  val equitiesForPortfolio = List("aapl","ibm", "xom", "goog")

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
      .put("script_lang", "native")
      .put("script.native.portfolioscript.type","org.hfgiii.sesportfolio.analytics.script.PortfolioNativeScriptFactory")
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

  def bulkIndexLoad(index:String,indexDefs:Seq[IndexDefinition],accumDocs:Long)(implicit client:ElasticClient) {

    client execute {
      bulk(indexDefs: _ *)
    }

    blockUntilCount(accumDocs,index)

  }

  def ror(ip:EquityPriceIndexAccumulator,sp:EquityPrices):Double =
    if(ip.lastClose == 0d || ip.lastEquity.compare(sp.name) != 0) 0d
    else  (sp.adj_close / ip.lastClose) - 1

  def newRoRIndex(indexNType:String)(ip:EquityPriceIndexAccumulator,sp:EquityPrices,rate_of_return:Double):(String,IndexDefinition) =
    ip.rorIndex.get(sp.date) match {
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
      equitiesForWeeklies.foldLeft(Map.empty[String,List[EquityPrices]]) {
        (lst,equity) =>
          val finput = this.getClass.getResourceAsStream(s"/${equity}_week.csv")

          val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

          lst + (equity ->  EquityPriceParser(equity,inputfile).parseEquities)

      }

    val weekZip = weeklyEquityPrices("msft").zip(weeklyEquityPrices("snp"))

    val idxAccumulator =
      weekZip.foldLeft(RoRWithSnPIndexAccumulator()) {
        (idx,tpl) =>
          val (msft,snp) = tpl

          val rors =
            (((idx.lastCloses._1,msft.adj_close),(idx.lastCloses._2,snp.adj_close)) map _ror).cast[(Double,Double)]

          rors.fold(idx) {
            rr =>

            val idxDefinition =
              index into "weekly_returns/weekly_ror" fields(
              "date" -> msft.date,
              "msft_ror" -> rr._1,
              "snp_ror" ->  rr._2)

            RoRWithSnPIndexAccumulator(lastCloses = (msft.adj_close, snp.adj_close),
              rorIndexDefinitions = idxDefinition :: idx.rorIndexDefinitions)
          }
        }

    val rorIndexed =
      idxAccumulator.rorIndexDefinitions.toSeq

    bulkIndexLoad("weekly_returns",rorIndexed,rorIndexed.length)

  }
  
  def loadDailyPrices(implicit client:ElasticClient):Int = {

    val dailyEquityPrices =

      equitiesForDailies.foldLeft(List.empty[EquityPrices]) {
        (lst,equity) =>
          val finput = this.getClass.getResourceAsStream(s"/${equity}_11.csv")

          val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

          lst ++ EquityPriceParser(equity,inputfile).parseEquities

      }

    val idxAccumulator =
      dailyEquityPrices.foldLeft(EquityPriceIndexAccumulator()) {
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

          val closingIndex = index into "sesportfolio/adj_close" fields (
               "date" -> sp.date,
               "symbol" -> sp.name,
               "adj_close" -> sp.adj_close
            )

          EquityPriceIndexAccumulator(
            lastEquity = sp.name,
            lastClose = sp.adj_close,
            equityIndex = equityIndex :: ip.equityIndex,
            closingIndex = closingIndex :: ip.closingIndex,
            rorIndex = ip.rorIndex + newDailyRoRIndex(ip,sp,rate_of_return))
      }

    val rorIndexed = idxAccumulator.rorIndex.values.toSeq

    bulkIndexLoad("daily_returns",rorIndexed,rorIndexed.length)


    val eqIndexed = idxAccumulator.equityIndex.toSeq

    bulkIndexLoad("sesportfolio",eqIndexed,eqIndexed.length)

    val clsIndexed = idxAccumulator.closingIndex.toSeq

    bulkIndexLoad("sesportfolio",clsIndexed,eqIndexed.length+clsIndexed.length)


    eqIndexed.length+clsIndexed.length
  }

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

  def loadPositions(equityNames:List[String],orderTransactions:String,seslength:Int)(implicit client:ElasticClient): Unit = {

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


    def firstOrder(date:String) = EquityOrder(date = date,symbol = "cash",ordertype = BuyToOpen,volume = 10000000l)
    val prices = groupPricesByDate(equityNames)
    val orders = groupOrdersByDate(orderTransactions)

    val tradingDates = prices.keys.toList.sorted

    val positions =
    tradingDates.foldLeft(List(initialPosition)) {
      (positions,date) =>
        orders.get(date).fold {
               updatePortfolio(date,List(firstOrder(date)),prices(date),positions)
        } {
           dailyOrders =>
             updatePortfolio(date,dailyOrders,prices(date),positions)
        }
    }

    val positionsIndexed =
        positions.map {
          pos =>
           val args = ("date" -> pos.date.get) :: pos.positions.map {
             p => p.symbol -> p.value
           }

           index into "sesportfolio/positions" fields (args: _*)
        }.toSeq


    bulkIndexLoad("sesportfolio",positionsIndexed,positionsIndexed.length+seslength)


  }
  
  def loadOrders(numDailyPrices:Int)(implicit client:ElasticClient):Int = {
    val finput = this.getClass.getResourceAsStream("/allorders.csv")

    val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

    val orders = EquityOrderParser(input = inputfile).parseEquityOrders

    val orderIndexed =
      orders.map {
         order =>
           index into "sesportfolio/order" fields (
              "date" -> order.date,
              "symbol" -> order.symbol,
              "ordertype" -> order.ordertype.name,
              "volume" -> order.volume
           )
      }.toSeq

    client execute {
      bulk(orderIndexed: _ *)
    }

    blockUntilCount(orderIndexed.length+numDailyPrices,"sesportfolio")

    orderIndexed.length+numDailyPrices
    
  }
  
  def loadUAProducts(implicit client:ElasticClient): Unit = {

    val finput = this.getClass.getResourceAsStream("/products.csv")

    val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

    val products = UAProductParser(input = inputfile).parseUAProducts

    val productsIndexed =
        products.map {
          product =>
            index into "products/product" fields (
              "code" -> product.code,
              "dtype" -> product.dtype,
              "level" -> product.level,
              "scode" -> product.scode
              )
        }.toSeq

    client execute {
      bulk(productsIndexed: _ *)
    }

    blockUntilCount(productsIndexed.length,"products")
    
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
            ),
           "adj_close" as (
             "date" typed DateType,
             "symbol" typed StringType,
             "adj_close" typed DoubleType
             ),
           "order" as(
             "date" typed DateType,
             "symbol" typed StringType,
             "orderType" typed StringType,
             "volume" typed LongType
             ),
           "portfolio" as (
              "date" typed DateType,
              "symbol" typed StringType,
              "shares" typed LongType,
              "value"  typed DoubleType
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

    val uaProductIdxResponse =
       client. execute {
         create index "products" mappings (
           "product" as (
              "code"  typed StringType index "not_analyzed",
              "dtype" typed StringType ,
              "level" typed IntegerType,
              "scode" typed StringType
             )

           )
       }.await


   // println(createIdxResponse.writeTo(new OutputStreamStreamOutput(System.out)))

    loadPositions(equitiesForPortfolio,"/allorders.csv",loadOrders(loadDailyPrices))

    loadWeeklyPrices

    loadUAProducts

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
    hitsFromSearchResponse {
      client.execute {
        search in "weekly_returns" types "weekly_ror" fields("msft_ror", "snp_ror") size 197 query matchall sort {
          by field "date" order SortOrder.DESC
        }
      }
     } {
        case hits:SearchHits => Option(hits.getHits.foldLeft(LinearRegressionArgs()) {
          (lr,hit) =>
            val RoRs(msft_ror,snp_ror) = fromHitFields[RoRs](hit)
            LinearRegressionArgs((snp_ror,1.0) :: lr.domain,msft_ror :: lr.codomain)
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

  def tradeSimulation(implicit client:ElasticClient) {
    val rors =
      hitsFromSearchResponse {
        client.execute {
          search in "sesportfolio" types "positions" query matchall size 256 sort {
            by field "date" order SortOrder.ASC
          } scriptfields(
            "balance" script "portfolioscript" lang "native" params Map("fieldName" -> "rate_of_return"),
            "date" script "doc['date'].value" lang "groovy"
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
