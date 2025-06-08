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
import scala.util.control.Breaks._

class GWRGeneralized(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends GWRbase(inputRDD){
  private var _tolerance = 1e-5
  private var _maxIter = 20
  private var _family: String = _

  // var for bw selection
  val select_eps = 1e-4
  private var opt_value: Array[Double] = _
  private var opt_result: Array[Double] = _
  private var opt_iters: Array[Double] = _

  private val epsilon = 1e-6 // 正则化常数，确保矩阵可逆

  def setParam(tol: Double, iter: Int)={
    _tolerance = tol
    _maxIter = iter
  }

  def setFamily(family: String)={
    _family = family
  }

  protected def bandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true, family: String = "poisson"): Double = {
    _family = family

    println("Selecting bandwidth...")
    // 检查数据量大小
    if (_rows > 1500) {
      println("Take a cup of tea and have a break, it will take a few minutes.")
      println("          -----A kind suggestion from GWmodel Lab")
    }

    if (adaptive) {
      adaptiveBandwidthSelection(kernel = kernel, approach = approach)
    } else {
      fixedBandwidthSelection(kernel = kernel, approach = approach)
    }
  }

  _disMax = 28574.81

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
      case e: Throwable => {
        println(f"meet error: ${e.getMessage}")
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
      case e: Throwable => {
        println(f"meet error ${e.getMessage}")
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
    val results = ggwr_wt(weight = newWeight)
    //    val yhat = results._2
    //    val residual = results._3
    val shat = results._4
    val trS = trace(shat)
    val n = _rows
    val llik = results._5
    // 计算GAICc
    val gaicc = -2 * llik + 2 * trS + 2 * trS * (trS + 1) / (n - trS - 1)
    val bw_print = if(_adaptive) bw.toInt else bw.formatted("%.2f")
    println(f"Bandwidth: ${bw_print}, AICc value: ${gaicc.formatted("%.5f")}")
    gaicc
  }

  private def bandwidthCV(bw: Double): Double = {
    val newWeight = if (_adaptive) {
      setWeight(round(bw), _kernel, _adaptive)
    } else {
      setWeight(bw, _kernel, _adaptive)
    }
    val cvWeight = modifyWeightCV(newWeight)
    val results = ggwr_wt(weight = cvWeight)
    val residual = results._3

    val cvDeviance =  residual.t * residual
    val bw_print = if(_adaptive) bw.toInt else bw.formatted("%.2f")
    println(f"Bandwidth: ${bw_print}, CV value: ${cvDeviance.formatted("%.5f")}")
    cvDeviance
  }

  private def modifyWeightCV(weightRDD: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    weightRDD.zipWithIndex.map { case (t, idx) =>
      val modifiedVector = t.copy
      modifiedVector(idx.toInt) = 0 // 只将当前点的权重设为0
      modifiedVector
    }
  }

  def ggwr_wt(X: DenseMatrix[Double] = _dmatX, Y: DenseVector[Double] = _dvecY, weight: RDD[DenseVector[Double]] = _spWeight)
  = {
    
    // 初始化
    var betas = Array.fill(_rows)(DenseVector.zeros[Double](_cols))
    var y_adj = DenseVector.zeros[Double](_rows)
    var yhat = DenseVector.zeros[Double](_rows)
    var residual = DenseVector.zeros[Double](_rows)
    var shat = DenseMatrix.zeros[Double](_rows, _rows)
    var llik = 0.0
    _family match {
        case "poisson" => {
            // 检查数据有效性
            if (Y.toArray.exists(_ < 0)) {
                throw new IllegalArgumentException("negative values not allowed for the 'Poisson' family")
            }
            // 初始化mu和eta
            var mu = Y + 0.1
            var nu = mu.map(math.log)
            // 初始化权重
            var wt2 = DenseVector.ones[Double](_rows)
            var old_llik = Double.NegativeInfinity
            //var llik = 0.0
            
            // IRWLS迭代
            breakable {
                for (iter <- 0 until _maxIter) {
                    // 计算工作变量
                    y_adj = nu + (Y - mu) / mu
                    // 计算加权最小二乘
                    val xtw = weight.map(w_sp => {
                        val each_col_mat = _dvecX.map(t => t * w_sp * wt2).flatMap(_.toArray)
                        new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
                    })
                    // 更新beta
                    betas = xtw.map(xtw => {
                        try {
                            val xtwx = xtw * X
                            val xtwy = xtw * y_adj
                            // 使用QR分解求解
                            val qr = breeze.linalg.qr.reduced(xtwx)
                            val beta = breeze.linalg.inv(qr.r) * (qr.q.t * xtwy)
                            beta
//                          val xtwx_inv = inv(xtwx)
//                          xtwx_inv * xtwy
                        } catch {
                            case e: MatrixSingularException =>
                                try {
                                    val regularized = inv(regularizeMatrix(xtw * X))
                                    regularized * xtw * y_adj
                                } catch {
                                    case e: Exception =>
                                        throw new IllegalStateException("Matrix inversion failed")
                                }
                        }
                    }).collect()
                    
                    // 更新eta和mu
                    nu = getYHat(X, betas)
                    mu = nu.map(math.exp)
                    // 计算对数似然
                    old_llik = llik
                    llik = Y.toArray.zip(mu.toArray).map { case (y, m) =>
                        if (y > 0) {
                          breeze.stats.distributions.Poisson(m).logProbabilityOf(y.toInt)
                        } else {
                            -m
                        }
                    }.sum
                  // 添加迭代输出
                  if (iter == 0) {
                    println(s"Iteration    Log-Likelihood")
                    println("=========================")
                  }
                  println(f"${iter}%8d    ${llik}%10.3f")

                    // 检查收敛性
                    if (math.abs((old_llik - llik) / llik) < _tolerance) {
                        yhat = mu
                        residual = Y - mu
                        break()
                    }
                    
                    // 更新权重
                    wt2 = mu
                    yhat = mu
                    residual = Y - mu
                }
            }
            
            // 计算帽子矩阵
            val ci: RDD[DenseMatrix[Double]] = weight.map(w_sp => {
                val each_col_mat = _dvecX.map(t => t * w_sp).flatMap(_.toArray)
                val xtw = new DenseMatrix(rows = _rows, cols = _cols, data = each_col_mat).t
                try {
                    val xtwx_inv = inv(xtw * X)
                    xtwx_inv * xtw
                } catch {
                    case e: MatrixSingularException =>
                        val regularized = inv(regularizeMatrix(xtw * X))
                        regularized * xtw
                }
            })
            
            val ci_idx = ci.zipWithIndex
            val si = ci_idx.map(t => {
                val a = X(t._2.toInt, ::).inner.toDenseMatrix
                val b = t._1.toDenseMatrix
                a * b
            })
            
            shat = DenseMatrix.create(
                rows = si.collect().length, 
                cols = si.collect().length, 
                data = si.collect().flatMap(_.toArray)
            )
            
            // 计算诊断统计量
            def diag(value: DenseVector[Double]) ={
              val n = value.length
              val res = DenseMatrix.eye[Double](n)
              for(i <- 0 until n) res(i,i) = value(i)
              res
            }
            val trS = trace(shat)
            val trStS = trace(shat * diag(wt2) * shat.t * diag(1.0 / wt2))
            val edf = _rows - 2 * trS + trStS
        }
        
        case "binomial" => {
            // 二项回归的实现
            // TODO: 实现二项回归的IRWLS
        }
    }
    (betas, yhat, residual, shat, llik)
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
  def GWPR(adaptive: Boolean, approach: String, kernel: String)={
    val var_n = _cols // number of variables
    val dp_n = _rows // number of samples
    val bw = bandwidthSelection(adaptive = adaptive, approach = approach, kernel = kernel,family = "poisson")

    println(f"bw: $bw")
  }

  // gw binomial reg
  def GWBR(adaptive: Boolean, approach: String, kernel: String)={

    val bw = bandwidthSelection(adaptive = adaptive, approach = approach, kernel = kernel,family = "binomial")

  }


}

object GWRGeneralized{
  def fit(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
          bandwidth: Double = -1, family: String = "poisson", kernel: String = "gaussian", adaptive: Boolean = true, cv: Boolean = true, approach: String = "AICc",
          tolerance: Double = 1e-05, maxIter: Int = 20)={

    val model = new GWRGeneralized(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    model.setParam(tolerance, maxIter)
    model.setFamily(family)
    // regression point
    val regPt = featureRDD

    // bandwidth selection if bw < 0


    // weight matrix


    family match {
      case "poisson" => model.GWPR(adaptive, approach, kernel)
      case "binomial" => model.GWBR(adaptive, approach, kernel)
      case _ => throw new IllegalStateException("Invalid family. Only \"poisson\" and \"binomial\" are valid.")
    }

    println("---finish---")

  }
}