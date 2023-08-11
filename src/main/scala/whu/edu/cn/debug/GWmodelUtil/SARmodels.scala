package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

//后续改写成抽象类
class SARmodels {

  protected var _X: Array[DenseVector[Double]] = _
  protected var _Y: DenseVector[Double] = _
  protected var _betas: DenseVector[Double] = _

  protected var geom: RDD[Geometry] = _
  var spweight_dvec: Array[DenseVector[Double]] = _
  var spweight_dmat: DenseMatrix[Double] = _


  def SARmodels() {

  }

  def calDiagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], betas: DenseVector[Double], res: DenseVector[Double], shat: Array[Double]) {
    val rss = sum(res.toArray.map(t => t * t))
    val n = X.rows.toDouble
    val mean_y = Y.toArray.sum / Y.toArray.length
    val AIC = n * log(rss / n) + n * log(2 * math.Pi) + n + shat(0)
//    AIC:-2*ll+2*p
//    -2*(-239.8491)+2*4
//    -2*LL+ 2*K*(n/(n-K-1))//这里的k=df
    val AICc = AIC + n * ((n + shat(0)) / (n - 2 - shat(0)))
    val edf = n - 2 * shat(0) + shat(1)
    val enp = 2 * shat(0) - shat(1)
    val yss = Y.toArray.map(t => (t - mean_y) * (t - mean_y)).sum
    println(mean_y)
    println(rss)
    val r2 = 1 - rss / yss
    val r2_adj = 1 - (1 - r2) * (n - 1) / (edf - 1)
    println(r2,r2_adj)
  }


  def init(inputRDD: RDD[(String, (Geometry, Map[String, Any]))]): Unit = {
    geom = getGeometry(inputRDD)
  }

  protected def setX(x: Array[DenseVector[Double]]) = {
    _X = x
  }

  protected def setY(y: Array[Double]) = {
    _Y = DenseVector(y)
  }

  protected def getdistance(): Array[Array[Double]] = {
    val coords = geom.map(t => t.getCoordinate)
    getCoorDistArrbuf(coords, coords).toArray
  }

  def fit() = {
    //    _x.foreach(println)
    //    _y.foreach(println)
    println("created")
  }

  def setcoords(lat: Array[Double], lon: Array[Double]) = {
    val geomcopy = geom.zipWithIndex()
    geomcopy.map(t => {
      t._1.getCoordinate.x = lat(t._2.toInt)
      t._1.getCoordinate.y = lon(t._2.toInt)
    })
    geom = geomcopy.map(t => t._1)
  }

  def setweight(neighbor: Boolean = true, k: Double = 0) = {
    if (neighbor && !geom.isEmpty()) {
      val nb_bool = getNeighborBool(geom)
      spweight_dvec = boolNeighborWeight(nb_bool).map(t => t * (t / t.sum)).collect()
    } else if (!neighbor && !geom.isEmpty() && k >= 0) {
      val dist = getdistance().map(t => Array2DenseVector(t))
      spweight_dvec = dist.map(t => getSpatialweightSingle(t, k, kernel = "boxcar", adaptive = true))
    }
    spweight_dmat = DenseMatrix.create(rows = spweight_dvec(0).length, cols = spweight_dvec.length, data = spweight_dvec.flatMap(t => t.toArray))
  }

  def get_logLik(res: DenseVector[Double]): Double = {
    val n = res.length
    val w = DenseVector.ones[Double](n)
    0.5 * (w.toArray.map(t => log(t)).sum - n * (log(2 * math.Pi) + 1.0 - log(n) + log((w * res * res).toArray.sum)))
  }

  def try_LRtest(LLx: Double, LLy: Double, chi_pama: Double = 1): Unit = {
    val score = 2.0 * (LLx - LLy)
    val pchi = breeze.stats.distributions.ChiSquared
    val pvalue = 1 - pchi.distribution(chi_pama).cdf(abs(score))
    println("ChiSquared", score, pvalue)
  }

}
