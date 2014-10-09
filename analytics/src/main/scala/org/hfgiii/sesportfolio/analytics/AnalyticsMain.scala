package org.hfgiii.sesportfolio.analytics

import scopt.OptionParser
import java.lang.System._


object AnalyticsMain {

  val  parser = new OptionParser[Opts]("analytics") {
    head("analytics", "0.1")
    opt[Boolean]('s', "sharperatio") action    { (x, o) =>
      o.copy(sharperatio = x,init = x)
    }
    opt[String]('e', "equity") action    { (x, o) =>
      if(!equitiesForDailies.contains(x)) {
        println(s"$x is not a valid equity for sharpe ratio calculation")
        o.copy(xit = true)
      } else {
        o.copy(equity = x)
      }
    }
    opt[Boolean]('o', "optimize") action { (x, o) =>
      o.copy(optimize = x,init = x)
    }

    opt[Boolean]('b', "beta") action { (x, o) =>
      o.copy(beta = x,init = x)
    }
  }

  def main(args:Array[String]): Unit = {
    parser.parse(args,Opts()) map {
      o =>

        implicit val client =
          if (o.init)
            initElasticsearch
          else
            emptyClient

        if (o.xit)
          shutdownElasticsearch
        else {

          if (o.sharperatio) {
            if (o.equity == "all") {
              allSharpeRatios
            } else {
              singleSharpRatio(o.equity)
            }
          }

          if (o.optimize) {
            optimize
          }

          if(o.beta) {
            betaCalculationForMSFT
          }

          shutdownElasticsearch
        }
    }
  }
}
