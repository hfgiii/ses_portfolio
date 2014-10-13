package org.hfgiii.sesportfolio.parser

/*
 * Copyright (C) 2014 Juergen Pfundt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.text.SimpleDateFormat

import com.novus.salat._
import com.novus.salat.global._
import org.hfgiii.sesportfolio.model._
import org.parboiled2._

import scala.util.{Failure, Success}

trait CSVParboiledParserSB[O] extends Parser with StringBuilding {
  val formatter = new SimpleDateFormat("yyyy-MM-dd")

  /* start of csv parser */
  def csvfile = rule{ (hdr ~ zeroOrMore(row)) ~> makeListOfOutputValues ~ zeroOrMore(optional("\r") ~ "\n") ~ EOI}
  def hdr = rule{ row }
  def row = rule{ oneOrMore(field).separatedBy(",") ~> genOutput ~ optional("\r") ~ "\n" }
  def field = rule{ string | text | MATCH ~> makeEmpty }
  def text = rule{ clearSB() ~ oneOrMore(noneOf(",\"\n\r")~ appendSB()) ~ push(sb.toString) ~> makeText }
  def string = rule{ WS ~ "\"" ~ clearSB() ~ zeroOrMore(("\"\"" | noneOf("\"")) ~ appendSB()) ~ push(sb.toString) ~> makeString ~ "\"" ~ WS }

  val whitespace = CharPredicate(" \t")
  def WS = rule{ zeroOrMore(whitespace) }

  def genOutput:(Seq[String]) => O

  def makeListOfOutputValues = (h: O, r: Seq[O]) => h::(r.toList:List[O])

  /* parser action */
  def makeText: (String) => String
  def makeString: (String) => String
  def makeEmpty: () => String

  def name:String
}

trait CSVParserAction {
  // remove leading and trailing blanks
  def makeText = (text: String) => text.trim
  // replace sequence of two double quotes by a single double quote
  def makeString = (string: String) => string.replaceAll("\"\"", "\"")
  // modify result of EMPTY token if required
  def makeEmpty = () => ""
}

trait CSVParserIETFAction extends CSVParserAction {
  // no trimming of WhiteSpace
  override def makeText = (text: String) => text
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
  def genOrderType(name:String):OrderType =
  if(name == "BUY")BuyToOpen
  else if(name == "SELL")SellToClose
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

case class CSVParboiledParserSBCLI( name:String, input: ParserInput) extends CSVParboiledParserEquityPrice with CSVParserIETFAction {
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

object CSVParserSBCLI  {

  def equities =
    List("aapl","amzn","ba","c","csco","dis","ebay","etfc","f","fdx",
         "ge","hd","hpq","jcp","jnj","k","l","msft","orcl","pfe","qcom","r","s",
         "t","twx","ups","v","vz","wag","x","xom","yhoo","zmh")

  def main(args: Array[String]) {
    equities.map { equity =>

      val finput = this.getClass.getResourceAsStream(s"/$equity.csv")

      lazy val inputfile : ParserInput = io.Source.fromInputStream(finput).mkString

      CSVParboiledParserSBCLI(equity,inputfile)

    }
  }
}
