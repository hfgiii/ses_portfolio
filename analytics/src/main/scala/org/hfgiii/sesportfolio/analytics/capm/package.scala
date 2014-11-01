package org.hfgiii.sesportfolio.analytics

import breeze.linalg.{DenseVector, DenseMatrix}
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import nak.regress.LinearRegression
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.sort.SortOrder
import org.hfgiii.ses.common.macros.SesMacros._
import org.hfgiii.ses.common.SesCommon._
import org.hfgiii.sesportfolio.analytics.model.RoRs

package object capm {

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

  def betaCalculationForMSFT(implicit client:ElasticClient) =
    hitsFromSearchResponse {
      client.execute {
        search in "weekly_returns" types "weekly_ror" fields("msft_ror", "snp_ror") size 197 query matchall sort {
          by field "date" order SortOrder.DESC
        }
      }
    } {
      case hits:SearchHits => Option(hits.getHits.foldLeft(List.empty[((Double,Double),Double)]) {
        (lr,hit) =>

          val RoRs(msft_ror,snp_ror) = fromHitFields[RoRs](hit)

          ((snp_ror,1.0) ,msft_ror) :: lr
      }.toSeq.unzip)

    } map { dc =>

      val (domain,codomain) = dc

      linearRegression(DenseMatrix(domain: _*),DenseVector(codomain: _*))
    }

}
