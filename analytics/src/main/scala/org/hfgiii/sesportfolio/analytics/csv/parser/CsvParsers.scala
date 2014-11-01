package org.hfgiii.sesportfolio.analytics.csv.parser

import com.novus.salat._
import com.novus.salat.global._
import org.hfgiii.ses.common.SesCommon._
import org.hfgiii.sesportfolio.analytics.model._
import org.parboiled2.{ParseError, ParserInput}

import scala.util.{Failure, Success}


trait CsvParsers {

  trait CSVParboiledParserUAProduct extends CSVParboiledParserSB[UAProduct] {
    def name = "product"
    def genOutput =
      (r: Seq[String]) =>
        UAProduct(
          code = r(0),
          dtype = r(1),
          level = r(2).toInt,
          scode = r(3)
        )
  }

  trait CSVParboiledParserEquityPrice extends CSVParboiledParserSB[EquityPrices] {

    def genOutput =
      (r: Seq[String]) =>
        EquityPrices (
          name = name,
          date = r(0),
          open = r(1).toDouble,
          high = r(2).toDouble,
          low  = r(3).toDouble,
          close = r(4).toDouble,
          volume = r(5).toLong,
          adj_close = r(6).toDouble
        )
  }

  trait CSVParboiledParserEquityOrder extends CSVParboiledParserSB[EquityOrder] {
    def name = "order"
    def genOrderType(name:String):OrderType =
      if(name == "Buy")BuyToOpen
      else if(name == "Sell")SellToClose
      else UnknownOrderType


    def normalize(un:String):String =
      if(un.size == 1)"0" + un
      else un

    def genOutput =
      (r: Seq[String]) =>
        EquityOrder(
          date = s"${r(0)}-${normalize(r(1))}-${normalize(r(2))}",
          symbol = r(3),
          ordertype = genOrderType(r(4)),
          volume = r(5).toLong

        )
  }

  case class CSVParboiledUAProductParserCLI(input: ParserInput) extends CSVParboiledParserUAProduct with CSVParserIETFAction {
    csvfile.run() match {
      case Success(result) => {
        val lists:List[UAProduct] = result.asInstanceOf[List[UAProduct]]

        lists.map {   uap =>
          println(grater[UAProduct].toPrettyJSON(uap)+",")
        }

      }
      case Failure(e: ParseError) => println("Expression is not valid: " + formatError(e))
      case Failure(e) => println("Unexpected error during parsing run: " + e)
    }
  }

  case class CSVParboiledEquityPriceParserCLI( name:String, input: ParserInput) extends CSVParboiledParserEquityPrice with CSVParserIETFAction {
    csvfile.run() match {
      case Success(result) => {
        val lists:List[EquityPrices] = result.asInstanceOf[List[EquityPrices]]

        lists.map {   sp =>
          println(grater[EquityPrices].toPrettyJSON(sp)+",")
        }

      }
      case Failure(e: ParseError) => println("Expression is not valid: " + formatError(e))
      case Failure(e) => println("Unexpected error during parsing run: " + e)
    }
  }

  case class CSVParboiledEquityOrderParserCLI(input: ParserInput) extends CSVParboiledParserEquityOrder with CSVParserIETFAction {

    //Salat context for serializing Ordertype case objects
    implicit val ctx = {
      val ctx = new Context {
        val name = "case_object_override"
      }
      ctx.registerCaseObjectOverride[OrderType, BuyToOpen.type]("Buy")
      ctx.registerCaseObjectOverride[OrderType, SellToClose.type]("Sell")
      ctx.registerCaseObjectOverride[OrderType, UnknownOrderType.type]("Unknown")
      ctx
    }

    csvfile.run() match {
      case Success(result) => {
        val lists:List[EquityOrder] = result.asInstanceOf[List[EquityOrder]]

        lists.map {   sp =>
          println(grater[EquityOrder].toPrettyJSON(sp)+",")
        }

      }
      case Failure(e: ParseError) => println("Expression is not valid: " + formatError(e))
      case Failure(e) => println("Unexpected error during parsing run: " + e)
    }
  }
}

object CsvParsers extends CsvParsers


object CSVEquityPriceParser  {

  import CsvParsers._

  def equities =
    List("aapl","amzn","ba","c","csco","dis","ebay","etfc","f","fdx",
      "ge","hd","hpq","jcp","jnj","k","l","msft","orcl","pfe","qcom","r","s",
      "t","twx","ups","v","vz","wag","x","xom","yhoo","zmh")

  def main(args: Array[String]) {
    equities.map { equity =>

      val finput = this.getClass.getResourceAsStream(s"/$equity.csv")

      lazy val inputfile : ParserInput = io.Source.fromInputStream(finput).mkString

      CSVParboiledEquityPriceParserCLI(equity,inputfile)

    }
  }
}

object CSVEquityOrderParser {

  import CsvParsers._

  def main(args: Array[String]) {

    val finput = this.getClass.getResourceAsStream(s"/orders.csv")

    lazy val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

    CSVParboiledEquityOrderParserCLI(inputfile)
  }
}

object CSVUAProductParser {

  import CsvParsers._

  def main(args: Array[String]) {

    val finput = this.getClass.getResourceAsStream(s"/products.csv")

    lazy val inputfile: ParserInput = io.Source.fromInputStream(finput).mkString

    CSVParboiledUAProductParserCLI(inputfile)
  }
}

