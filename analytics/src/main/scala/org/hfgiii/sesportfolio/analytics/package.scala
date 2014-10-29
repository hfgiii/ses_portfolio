package org.hfgiii.sesportfolio

import java.io.File
import java.util.UUID
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._
import com.sksamuel.elastic4s.{ElasticClient, IndexDefinition}
import org.elasticsearch.common.settings.ImmutableSettings
import org.hfgiii.ses.common.csv.parser.CSVParserIETFAction
import org.hfgiii.ses.common.macros.SesMacros._
import org.hfgiii.ses.common.SesCommon._
import org.hfgiii.sesportfolio.analytics.model._
import org.hfgiii.sesportfolio.analytics.simulation._
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

    val client = initLocalEs4sClient(settings)

    loadIndexes(client)

    client
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

  def loadPositions(equityNames:List[String],orderTransactions:String,seslength:Int)(implicit client:ElasticClient): Unit = {

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

}
