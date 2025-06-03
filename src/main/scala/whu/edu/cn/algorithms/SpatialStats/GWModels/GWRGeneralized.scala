package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, qr, sum, trace}
import breeze.plot.{Figure, plot}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance.getDist
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight.{Array2DenseVector, getSpatialweight, getSpatialweightSingle}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._
import whu.edu.cn.util.ShapeFileUtil.{builderFeatureType, readShp}
import whu.edu.cn.oge.Service

import java.awt.Graphics2D
import java.awt.image.BufferedImage
import scala.collection.mutable

import scala.collection.mutable

class GWRGeneralized(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends GWRbase(inputRDD){
  private var _tolerance = 0.0
  private var _maxIter = 0

  // var for bw selection
  val select_eps = 1e-2
  private var opt_value: Array[Double] = _
  private var opt_result: Array[Double] = _
  private var opt_iters: Array[Double] = _

  private val epsilon = 1e-6 // 正则化常数，确保矩阵可逆

  def setParam(tol: Double, iter: Int)={
    _tolerance = tol
    _maxIter = iter
  }

  protected def bandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true): Double = {
    if (adaptive) {
      adaptiveBandwidthSelection(kernel = kernel, approach = approach)
    } else {
      fixedBandwidthSelection(kernel = kernel, approach = approach)
    }
  }

  private def fixedBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Double = _disMax, lower: Double = _disMax / 5000.0): Double = {
    _kernel = kernel
    _adaptive = false
    var bw: Double = lower
    var approachfunc: Double => Double = bandwidthAICc
    if (approach == "CV") {
      approachfunc = bandwidthCV
    }
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
      bw = re._1
    } catch {
      case e: MatrixSingularException => {
        val low = lower * 2
        bw = fixedBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def adaptiveBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Int = _rows - 1, lower: Int = 20): Int = {
    _kernel = kernel
    _adaptive = true
    var bw: Int = lower
    var approachfunc: Double => Double = bandwidthAICc
    if (approach == "CV") {
      approachfunc = bandwidthCV
    }
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
      bw = re._1.toInt
    } catch {
      case e: MatrixSingularException => {
        println("meet matrix singular error")
        val low = lower + 1
        bw = adaptiveBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def bandwidthAICc(bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val results = fitFunction(weight = newWeight)
    val residual = results._3
    val shat = results._4
    val shat0 = trace(shat)
    val rss = residual.toArray.map(t => t * t).sum
    val n = _rows
    n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
  }

  private def bandwidthCV(bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val cvWeight = modifyWeightCV(newWeight)
    val results = fitFunction(weight = cvWeight)
    val residual = results._3
    residual.toArray.map(t => t * t).sum
  }

  //这个是有问题的
  private def modifyWeightCV(weightRDD: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    weightRDD.map { t =>
      val modifiedVector = t.copy
      for (i <- 0 until t.length) {
        modifiedVector(i) = 0
      }
      modifiedVector
    }
  }

  def fitFunction(X: DenseMatrix[Double] = _dmatX, Y: DenseVector[Double] = _dvecY, weight: RDD[DenseVector[Double]] = _spWeight):
  (Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double]) = {

    val xtw = weight.map(w => {
      val each_col_mat = _dvecX.map(t => t * w).flatMap(_.toArray)
      new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
    })
    //    val xtw0=xtw.collect()
    val betas = xtw.map(xtw => {
      try {
        val xtwx = xtw * X
        val xtwy = xtw * Y
        val xtwx_inv = inv(xtwx)
        xtwx_inv * xtwy
      } catch {
        case e: breeze.linalg.MatrixSingularException =>
          try {
            val regularized = inv(regularizeMatrix(xtw * X))
            regularized * xtw * Y
          } catch {
            case e: Exception =>
              throw new IllegalStateException("Matrix inversion failed")
          }
        case e: Exception =>
          println(s"An unexpected error occurred: ${e.getMessage}")
          DenseVector.zeros[Double](_cols)
      }
    })
    //    val betas0 = betas.collect()
    //    betas0.foreach(println)
    val ci: RDD[DenseMatrix[Double]] = xtw.map(t => {
      try {
        val xtwx_inv = inv(t * X)
        xtwx_inv * t // 继续进行矩阵乘法
      } catch {
        case e: breeze.linalg.MatrixSingularException =>
          try {
            val regularized = inv(regularizeMatrix(t * X)) // 先进行正则化，再求逆
            regularized * t
          } catch {
            case e: Exception =>
              throw new IllegalStateException("Matrix inversion failed")
          }
        case e: Exception =>
          throw new RuntimeException("An unexpected error occurred during matrix computation.")
      }
    })
    val ci_idx = ci.zipWithIndex
    //    val sum_ci = ci.map(t => t.map(t => t * t)).map(t => sum(t(*, ::)))
    val si = ci_idx.map(t => {
      val a = X(t._2.toInt, ::).inner.toDenseMatrix
      val b = t._1.toDenseMatrix
      a * b
    })
    val shat = DenseMatrix.create(rows = si.collect().length, cols = si.collect().length, data = si.collect().flatMap(_.toArray))
    val yhat = getYHat(X, betas.collect())
    val residual = Y - yhat
    //    val s = calDiagnostic(X, Y, residual, shat)
    //    println(s)
    (betas.collect(), yhat, residual, shat)
  }

  protected def getYHat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
    val betas_idx = betas.zipWithIndex
    val yhat = betas_idx.map(t => {
      sum(t._1 * X(t._2, ::).inner)
    })
    DenseVector(yhat)
  }

  def regularizeMatrix(matrix: DenseMatrix[Double]): DenseMatrix[Double] = {
    val eye = DenseMatrix.eye[Double](matrix.rows)
    matrix + eye * epsilon //添加epsilon
  }

  //---------------------------------------------------------------------------------------------

  // gw poisson reg
  def GWPR()={
    val var_n = _cols // number of variables
    val dp_n = _rows // number of samples

  }

  // gw binomial reg
  def GWBR()={

  }


}

object GWRGeneralized{
  def fit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
          bandwidth: Double = -1, family: String = "poisson", kernel: String = "gaussian", adaptive: Boolean = true, cv: Boolean = true,
          tolerance: Double = 1e-05, maxIter: Int = 20)={

    val model = new GWRGeneralized(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    model.setParam(tolerance, maxIter)
    // regression point
    val regPt = featureRDD

    // bandwidth selection if bw < 0


    // weight matrix


    family match {
      case "poisson" => model.GWPR()
      case "binomial" => model.GWBR()
      case _ => throw new IllegalStateException("Invalid family. Only \"poisson\" and \"binomial\" are valid.")
    }

    println("---finish---")

  }
}