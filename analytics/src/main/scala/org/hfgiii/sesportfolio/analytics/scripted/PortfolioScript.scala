package org.hfgiii.sesportfolio.analytics.scripted

import java.util.{Map => JMap}

import org.elasticsearch.common.Nullable
import org.elasticsearch.script.AbstractDoubleSearchScript
import shapeless.syntax.typeable._

import scala.collection.JavaConversions._


class PortfolioScript(@Nullable params: JMap[String, AnyRef]) extends AbstractDoubleSearchScript {


   override def runAsDouble:Double = {
     val tot =
     source.entrySet.foldLeft(0d) {
       (value,x) =>
         val entry = x.cast[java.util.Map.Entry[String,AnyRef]]
         entry.get.getKey match {
           case "date" => println(s"key:${entry.get.getKey} and value:${entry.get.getValue}") ; value
           case _ => entry.get.getValue.cast[Double].fold(value)(_ + value)
         }
     }

     println(s"tot = $tot")

     tot
   }

}
