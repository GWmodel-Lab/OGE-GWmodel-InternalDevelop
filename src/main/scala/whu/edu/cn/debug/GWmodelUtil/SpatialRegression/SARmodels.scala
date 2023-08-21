package whu.edu.cn.debug.GWmodelUtil.SpatialRegression

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.debug.GWmodelUtil.Utils.FeatureDistance._
import whu.edu.cn.debug.GWmodelUtil.Utils.FeatureSpatialWeight._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._

//改写成抽象类，不可以被初始化
abstract class SARmodels {

  private var shpRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))] = _
  protected var _X: Array[DenseVector[Double]] = _
  protected var _Y: DenseVector[Double] = _

  protected var geom: RDD[Geometry] = _
  protected var spweight_dvec: Array[DenseVector[Double]] = _
  protected var spweight_dmat: DenseMatrix[Double] = _

  var fitvalue: Array[Double] = _

  def SARmodels() {

  }

  protected def calDiagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], residuals: DenseVector[Double], loglikelihood: Double, df: Double) {
    val n = X.rows.toDouble
    val rss = sum(residuals.toArray.map(t => t * t))
    val mean_y = Y.toArray.sum / Y.toArray.length
    val AIC = -2 * loglikelihood + 2 * df
    val AICc = -2 * loglikelihood + 2 * df * (n / (n - df - 1))
    val yss = Y.toArray.map(t => (t - mean_y) * (t - mean_y)).sum
    val r2 = 1 - rss / yss
    val r2_adj = 1 - (1 - r2) * (n - 1) / (n - df - 1)
    println(s"diagnostics:\nSSE is $rss\nLog likelihood is $loglikelihood\nAIC is $AIC \nAICc is $AICc\nR2 is $r2\nadjust R2 is $r2_adj")
  }


  def init(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], style: String = "W"): Unit = {
    geom = getGeometry(inputRDD)
    shpRDD = inputRDD
    setweight(style = style)
  }

  protected def setX(x: Array[DenseVector[Double]]): Unit = {
    _X = x
  }

  protected def setY(y: Array[Double]): Unit = {
    _Y = DenseVector(y)
  }

  protected def getdistance(): Array[Array[Double]] = {
    val coords = geom.map(t => t.getCoordinate)
    getCoorDistArrbuf(coords, coords).toArray
  }

  def setcoords(lat: Array[Double], lon: Array[Double]): Unit = {
    val geomcopy = geom.zipWithIndex()
    geomcopy.map(t => {
      t._1.getCoordinate.x = lat(t._2.toInt)
      t._1.getCoordinate.y = lon(t._2.toInt)
    })
    geom = geomcopy.map(t => t._1)
  }

  def setweight(neighbor: Boolean = true, k: Double = 0, style: String = "W"): Unit = {
    if (neighbor && !geom.isEmpty()) {
      //      val nb_bool = getNeighborBool(geom)
      //      spweight_dvec = boolNeighborWeight(nb_bool).map(t => t * (t / t.sum)).collect()
      spweight_dvec = getNeighborWeight(shpRDD, style = style).collect()
    } else if (!neighbor && !geom.isEmpty() && k >= 0) {
      val dist = getdistance().map(t => Array2DenseVector(t))
      spweight_dvec = dist.map(t => getSpatialweightSingle(t, k, kernel = "boxcar", adaptive = true))
    }
    spweight_dmat = DenseMatrix.create(rows = spweight_dvec(0).length, cols = spweight_dvec.length, data = spweight_dvec.flatMap(t => t.toArray))
  }

  def printweight(): Unit = {
    spweight_dvec.foreach(println)
  }

  protected def get_logLik(res: DenseVector[Double]): Double = {
    val n = res.length
    val w = DenseVector.ones[Double](n)
    0.5 * (w.toArray.map(t => log(t)).sum - n * (log(2 * math.Pi) + 1.0 - log(n) + log((w * res * res).toArray.sum)))
  }

  protected def try_LRtest(LLx: Double, LLy: Double, chi_pama: Double = 1): Unit = {
    val score = 2.0 * (LLx - LLy)
    val pchi = breeze.stats.distributions.ChiSquared
    val pvalue = 1 - pchi.distribution(chi_pama).cdf(abs(score))
    println(s"ChiSquared test, score is $score, p value is $pvalue")
  }

  protected def betasMap(coef: DenseVector[Double]): mutable.Map[String, Double] = {
    val arrbuf = new ArrayBuffer[String]()
    arrbuf += "Intercept"
    for (i <- 1 until coef.length) {
      val tmp = "X" + i.toString
      arrbuf += tmp
    }
    val coefname = arrbuf.toArray
    val coefvalue = coef.toArray
    val betas_map: mutable.Map[String, Double] = mutable.Map()
    for (i <- 0 until coef.length) {
      betas_map += (coefname(i) -> coefvalue(i))
    }
    //    println(betas_map)
    betas_map
  }

}