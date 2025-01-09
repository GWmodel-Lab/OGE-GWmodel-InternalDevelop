package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg.{DenseMatrix, DenseVector, max}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.GWModels.Algorithm
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.LogisticRegression.setX
import whu.edu.cn.oge.Service

import scala.collection.mutable

object PoissonRegression extends Algorithm {
  private var _data: RDD[mutable.Map[String, Any]] = _
  private var _dmatX: DenseMatrix[Double] = _
  private var _dvecY: DenseVector[Double] = _

  private var _rawX: Array[Array[Double]] = _
  private var _rawdX: DenseMatrix[Double] = _

  private var _nameX: Array[String] = _
  private var _nameY: String = _
  private var _rows: Int = 0
  private var _df: Int = 0

  override def setX(properties: String, split: String = ","): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      _data.map(t => t(s).asInstanceOf[String].toDouble).collect()
    })
    _rows = x(0).length
    _df = x.length

    _rawX = x
    _rawdX = DenseMatrix.create(rows = _rows, cols = x.length, data = x.flatten)
    val onesX = Array(DenseVector.ones[Double](_rows).toArray, x.flatten)
    _dmatX = DenseMatrix.create(rows = _rows, cols = x.length + 1, data = onesX.flatten)
  }

  override def setY(property: String): Unit = {
    _nameY = property
    _dvecY = DenseVector(_data.map(t => t(property).asInstanceOf[String].toDouble).collect())
  }

  def fit(sc: SparkContext, data: RDD[(String, (Geometry, mutable.Map[String, Any]))],
          y: String, x: String, Intercept: Boolean = true,
          maxIter: Int = 100,epsilon: Double = 1e-8): Unit = {
    _data = data.map(t => t._2._2)
    setX(x)
    setY(y)
    val X = if (Intercept) _dmatX else _rawdX
    val Y = _dvecY

    // 检查数据有效性
    if (Y.toArray.exists(_ < 0)) {
      throw new IllegalArgumentException("negative values not allowed for the 'Poisson' family")
    }

    // 初始化
    // R中使用 mustart <- y + 0.1
    var mu = Y + 0.1
    var eta = mu.map(math.log) // linkfun = log
    var beta = DenseVector.zeros[Double](X.cols)
    val weights = DenseVector.ones[Double](Y.length) // prior weights

    var devold = Double.PositiveInfinity
    var dev = calculateDeviance(Y, mu, weights)
    var iter = 0
    var converged = false

    while (iter < maxIter && !converged) {
      iter += 1
      devold = dev

      val varmu = mu.copy // variance = mu for Poisson
      val muEta = mu.copy // mu.eta = mu for log link
      // adjusted response
      val z = eta + (Y - mu)/muEta
      // working weights
      val w = muEta
      //val w = multipleByElement(weights, divideByElement(varmu, mu.map(x => math.pow(math.log(x), 2))))// 创建权重矩阵

      val W = DenseMatrix.zeros[Double](Y.length, Y.length)
      for (i <- 0 until Y.length) {
        W(i, i) = math.sqrt(w(i))
      }

      try {
        val Xw = W * X
        val zw = W * z
        // 使用QR分解求解加权最小二乘
        val qr = breeze.linalg.qr.reduced(Xw)
        beta = breeze.linalg.inv(qr.r) * (qr.q.t * zw)
        //beta = breeze.linalg.backsolve(qr.r, qr.q.t * zw)
        // 更新线性预测值
        eta = X * beta
        // 更新均值 (linkinv = exp)
        mu = eta.map(math.exp)
        // 计算新的偏差
        dev = calculateDeviance(Y, mu, weights)
        // 检查收敛性
        converged = math.abs((dev - devold) / (dev + 0.1)) < epsilon

      } catch {
        case e: Exception =>
          println(s"Error in iteration $iter: ${e.getMessage}")
          converged = true
      }
    }

    //yhat, residual
    val yhat = exp(X * beta)
    val res = (Y - yhat)

    // deviance residuals
    val devRes = DenseVector.zeros[Double](Y.length)
    for (i <- 0 until Y.length) {
      val yi = Y(i)
      val yhat_i = yhat(i)

      if(yi==0){
        devRes(i) = -math.sqrt(2.0 * yhat_i)
      }else{
        val dev = 2.0 * (yi * math.log(yi/yhat_i) - (yi - yhat_i))
        devRes(i) = math.signum(yi-yhat_i) * math.sqrt(dev)
      }
    }

    // printed string
    var str = "\n********************Results of Poisson Regression********************\n"

    var formula = f"${y} ~ "
    for (i <- 1 until X.cols) {
      if (i == 1) {
        formula += f"${_nameX(i - 1)} "
      } else {
        formula += f"+ ${_nameX(i - 1)} "
      }
    }
    str += "Formula:\n" + formula + f"\n"

    str += "\n"
    str += f"Deviance Residuals: \n" +
      f"min: ${devRes.toArray.min.formatted("%.4f")}  " +
      f"max: ${devRes.toArray.max.formatted("%.4f")}  " +
      f"mean: ${breeze.stats.mean(devRes).formatted("%.4f")}  " +
      f"median: ${breeze.stats.median(devRes).formatted("%.4f")}\n"

    str += "\n"
    str += "Coefficients:\n"
    if (Intercept) {
      str += f"Intercept:${beta(0).formatted("%.6f")}\n"
      for (i <- 1 until (X.cols)) {
        str += f"${_nameX(i - 1)}: ${beta(i).formatted("%.6f")}\n"
      }
    }else{
      for (i <- 0 until (X.cols)) {
        str += f"${_nameX(i - 1)}: ${beta(i).formatted("%.6f")}\n"
      }
    }// need to fix at linear and logistic

    str += "\n"
    str += f"Number of Iterations: ${iter}\n"

    str += "**********************************************************************\n"

    Service.print(str,"Poisson Regression for feature","String")
  }

  protected def diagnostic(X: DenseMatrix[Double], Y: DenseVector[Double], devRes: DenseVector[Double], df: Double): String = {

    ""
  }

  private def calculateDeviance(y: DenseVector[Double], mu: DenseVector[Double],
                                weights: DenseVector[Double]): Double = {
    var dev = 0.0
    for (i <- 0 until y.length) {
      if (y(i) > 0) {
        dev += weights(i) * (y(i) * math.log(y(i)/mu(i)) - (y(i) - mu(i)))
      } else {
        dev += weights(i) * mu(i)
      }
    }
    2.0 * dev
  }

  private def multipleByElement (a: DenseVector[Double],b:DenseVector[Double]): DenseVector[Double] = {
    val res = a.copy
    for(i<- 0 until res.length){
      res(i) = a(i)*b(i)
    }
    res
  }

  private def divideByElement(a: DenseVector[Double], b: DenseVector[Double]): DenseVector[Double] = {
    val res = a.copy
    for (i <- 0 until res.length) {
      res(i) = a(i) / b(i)
    }
    res
  }
}
