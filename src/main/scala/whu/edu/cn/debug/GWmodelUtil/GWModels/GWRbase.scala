package whu.edu.cn.debug.GWmodelUtil.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, det, eig, inv, qr, sum, trace}
import breeze.stats.mean
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.debug.GWmodelUtil.Utils.FeatureDistance._
import whu.edu.cn.debug.GWmodelUtil.Utils.FeatureSpatialWeight._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._

class GWRbase {

  private var shpRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))] = _
  protected var _X: Array[DenseVector[Double]] = _
  protected var _Y: DenseVector[Double] = _

  protected var geom: RDD[Geometry] = _
  protected var spweight_dvec: Array[DenseVector[Double]] = _
//  protected var spweight_dmat: DenseMatrix[Double] = _

  protected var max_dist: Double = _
  var _kernel:String=_
  var _adaptive:Boolean=_
  var _dist: Array[DenseVector[Double]]=_
  var fitvalue: Array[Double] = _

  protected def calDiagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], residual: DenseVector[Double], shat: DenseMatrix[Double]): Unit = {
    val shat0 = trace(shat)
    val shat1 = trace(shat * shat.t)
    val rss = residual.toArray.map(t => t * t).sum
    val n = X.rows
    val AIC = n * log(rss / n) + n * log(2 * math.Pi) + n + shat0
    val AICc = n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
    val edf = n - 2.0 * shat0 + shat1
    val enp = 2.0 * shat0 - shat1
    val yss = sum((Y - mean(Y)) * (Y - mean(Y)))
    val r2 = 1 - rss / yss
    val r2_adj = 1 - (1 - r2) * (n - 1) / (edf - 1)
    println(s"diagnostics:\nSSE is $rss\nAIC is $AIC \nAICc is $AICc\nedf is $edf \nenp is $enp\nR2 is $r2\nadjust R2 is $r2_adj")
  }


  def init(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): Unit = {
    geom = getGeometry(inputRDD)
    shpRDD = inputRDD
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

  def setweight(bw:Double, kernel:String, adaptive:Boolean): Unit = {
    if(_dist==null){
      _dist = getdistance().map(t => Array2DenseVector(t))
    }
    if(_kernel==null) {
      _kernel=kernel
      _adaptive=adaptive
    }
    //find max_dist
    spweight_dvec = _dist.map(t => getSpatialweightSingle(t, bw = bw, kernel = kernel, adaptive = adaptive))
//    spweight_dmat = DenseMatrix.create(rows = spweight_dvec(0).length, cols = spweight_dvec.length, data = spweight_dvec.flatMap(t => t.toArray))
  }

  def printweight(): Unit = {
    spweight_dvec.foreach(println)
  }

  def getAICc(X: DenseMatrix[Double], Y: DenseVector[Double], residual: DenseVector[Double], shat: DenseMatrix[Double]):Double= {
    val shat0 = trace(shat)
    val rss = residual.toArray.map(t => t * t).sum
    val n = X.rows
    n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
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
