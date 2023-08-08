package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector}
import scala.math._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.{ArrayBuffer, Map}
import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

class SARlagmodel extends SARmodels {

  var _betas: DenseVector[Double] = _

  var _xlength = 0
  var _dX: DenseMatrix[Double] = _

  var lm_null: DenseVector[Double] = _
  var lm_w: DenseVector[Double] = _
  var _wy: DenseVector[Double] = _
  var _eigen: eig.DenseEig = _

  //  override def init(inputRDD: RDD[(String, (Geometry, Map[String, Any]))]): Unit = {
  //    geom = getGeometry(inputRDD)
  //  }

  override def setX(x: Array[DenseVector[Double]]) = {
    _X = x
    _xlength = _X(0).length
    _dX = DenseMatrix.create(rows = _xlength, cols = _X.length, data = _X.flatMap(t => t.toArray))
  }

  override def setY(y: Array[Double]) = {
    _Y = DenseVector(y)
  }

  override def fit(): Unit = {

    val inte = getinterval()
    val rho = goldenSelection(inte._1, inte._2)
    val yy = _Y - rho * _wy
    val betas = get_betas(Y = yy)
    val res = specify_res(Y = yy)
    val fit = _Y - res
    val SSE = sum(res.toArray.map(t => t * t))
    val s2 = SSE / _xlength
    //    println(fit)
    nelderMead(rho, betas)
  }

  def get_betas(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xlength)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    betas
  }

  def specify_res(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xlength)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    val y_hat = X * betas
    Y - y_hat
  }

  def get_env(): Unit = {
    if (lm_null == null || lm_w == null || _wy == null) {
      _wy = DenseVector(spweight_dvec.map(t => (t dot _Y)))
      //      wy.foreach(println)
      lm_null = specify_res()
      lm_w = specify_res(Y = _wy)
    }
    if (_eigen == null) {
      _eigen = breeze.linalg.eig(spweight_dmat.t)
    }
  }

  def rho4optimize(rho: Double): Double = {
    get_env()
    val e_a = lm_null.t * lm_null
    val e_b = lm_w.t * lm_null
    val e_c = lm_w.t * lm_w
    val SSE = e_a - 2.0 * rho * e_b + rho * rho * e_c
    val n = _xlength
    val s2 = SSE / n
    val eigvalue = _eigen.eigenvalues.copy
    val eig_rho = eigvalue :*= rho
    val eig_rho_cp = eig_rho.copy
    val ldet = sum(breeze.numerics.log(-eig_rho_cp :+= 1.0))
    val ret = (ldet - ((n / 2) * log(2 * math.Pi)) - (n / 2) * log(s2) - (1 / (2 * s2)) * SSE)
    //    println(ret)
    ret
  }

  def getinterval(): (Double, Double) = {
    get_env()
    val eigvalue = _eigen.eigenvalues.copy
    val min = eigvalue.toArray.min
    val max = eigvalue.toArray.max
    (1.0 / min, 1.0 / max)
  }

  def goldenSelection(lower: Double, upper: Double, eps: Double = 1e-10): Double = {
    var iter: Int = 0
    val max_iter = 1000

    val ratio: Double = (sqrt(5) - 1) / 2.0
    var a = lower + 1e-12
    var b = upper - 1e-12
    var step = b - a
    var p = a + (1 - ratio) * step
    var q = a + ratio * step
    var f_a = rho4optimize(a)
    var f_b = rho4optimize(b)
    var f_p = rho4optimize(p)
    var f_q = rho4optimize(q)
    //    println(f_a,f_b,f_p,f_q)
    while (abs(f_a - f_b) >= eps && iter < max_iter) {
      if (f_p > f_q) {
        b = q
        f_b = f_q
        q = p
        f_q = f_p
        step = b - a
        p = a + (1 - ratio) * step
        f_p = rho4optimize(p)
      } else {
        a = p
        f_a = f_p
        p = q
        f_p = f_q
        step = b - a
        q = a + ratio * step
        f_q = rho4optimize(q)
      }
      iter += 1
    }
    println((b + a) / 2.0)
    (b + a) / 2.0
  }

  def lagsse4optimize(rho: Double, betas: DenseVector[Double]): Double = {
    val res = _Y - rho * _wy - _dX * betas
    val SSE = sum(res.toArray.map(t => t * t))
    val n = _xlength
    val s2 = SSE / n
    val eigvalue = _eigen.eigenvalues.copy
    val eig_rho = eigvalue :*= rho
    val eig_rho_cp = eig_rho.copy
    val det = sum(breeze.numerics.log(-eig_rho_cp :+= 1.0))
    val ret = (det - ((n / 2) * log(2 * math.Pi)) - (n / 2) * log(s2) - (1 / (2 * s2)) * SSE)
    ret
  }

  //  def get_ret(rho:Double, SSE: Double): Double = {
  //    val n = _xlength
  //    val s2 = SSE / n
  //    val eigvalue = _eigen.eigenvalues.copy
  //    val eig_rho = eigvalue :*= rho
  //    val eig_rho_cp = eig_rho.copy
  //    val det = sum(breeze.numerics.log(-eig_rho_cp :+= 1.0))
  //    val ret = (det - ((n / 2) * log(2 * math.Pi)) - (n / 2) * log(s2) - (1 / (2 * s2)) * SSE)
  //    ret
  //  }

  def nelderMead(rho: Double, betas: DenseVector[Double])= {
    var iter = 0
    val max_iter = 1000
    val th_eps = 1e-10
    val optdata: Array[Array[Double]] = Array(Array(rho), betas.toArray)
    val optParameter = optdata.flatten

    //如果是3维，算上初始点一共3+1个点，m=3。但是，又因为点数从0开始算，m作为下标，实际应该是3-1=2
    val m = optParameter.length - 1

    var optArr = new ArrayBuffer[Array[Double]]
    optArr += optParameter  //先放入没有变化的，第0个
    for (i <- 0 until optParameter.length) {
      val tmp = optParameter.clone()
      tmp(i) = tmp(i) * 1.05
      optArr += tmp
    }
    //    optArr.map(t=>t.foreach(println))
    //存储四个点的优化目标函数值，0代表第一次，只用一次。
    val re_lagsse0 = optArr.toArray.map(t => -lagsse4optimize(t(0), DenseVector(t.drop(1))))
    var arr_lagsse = re_lagsse0.clone()
    var eps = 1.0
    //这个是所有的arr
    var ord_Arr = optArr.clone()

    while (eps >= th_eps && iter < max_iter) {

      //排序，从小到大
      val re_lagsse_idx = arr_lagsse.zipWithIndex.sorted
      //      re_lagsse_idx.foreach(println)
      val ord_0 = ord_Arr(re_lagsse_idx(0)._2)
      val ord_m = ord_Arr(re_lagsse_idx(m)._2)
      val ord_m1 = ord_Arr(re_lagsse_idx(m + 1)._2)
      //如果第m+1的点需要改变，这个是为了放进数组里
      var ord_m1_change = ord_m1.clone()
      //      ord_m1.foreach(println)
      val lagsse_0 = -lagsse4optimize(ord_0(0), DenseVector(ord_0.drop(1)))
      val lagsse_m = -lagsse4optimize(ord_m(0), DenseVector(ord_m.drop(1)))
      val lagsse_m1 = -lagsse4optimize(ord_m1(0), DenseVector(ord_m1.drop(1)))
      //求点0和m+1的差
      val dif = DenseVector(ord_m1) - DenseVector(ord_0)
      eps = sqrt(dif.toArray.map(t => t * t).sum)
      println(s"the iter is $iter, the difference is $dif, the eps is $eps")

      //这个是前m个的arr，从0到m
      var ord_0mArr = new ArrayBuffer[Array[Double]]
      for (i <- 0 until m + 1) {
        val tmp = ord_Arr(re_lagsse_idx(i)._2)
        ord_0mArr += tmp
      }
      //这个是1到m+1个的arr,不包括第0
      var ord_1m1Arr = new ArrayBuffer[Array[Double]]
      for (i <- 1 until m + 1 + 1) {
        val tmp = ord_Arr(re_lagsse_idx(i)._2)
        ord_1m1Arr += tmp
      }
      var flag_A10: Boolean = false
      //质心c
      val c = nm_gravityCenter(ord_0mArr.toArray)
      //反射点r
      val r = (DenseVector(c) + 0.99999*(DenseVector(c) - DenseVector(ord_m1))).toArray
      val lagsse_r = -lagsse4optimize(r(0), DenseVector(r.drop(1))) //计算r点的sse

      ord_Arr.clear()
      ord_Arr = ord_0mArr.clone() //前m个点已经放进来了

      if (lagsse_r <= lagsse_0) {
        //拓展点s
        val s = (DenseVector(c) + 2.0 * (DenseVector(c) - DenseVector(ord_m1))).toArray
        val lagsse_s = -lagsse4optimize(s(0), DenseVector(s.drop(1)))
        if (lagsse_s <= lagsse_r) {
          ord_m1_change = s
        } else {
          ord_m1_change = r
        }
      } else if (lagsse_r > lagsse_0 && lagsse_r <= lagsse_m) {
//        println(c.toVector)
//        println(r.toVector)
//        println(ord_m1.toVector)
        ord_m1_change = r
      } else if (lagsse_r > lagsse_m && lagsse_r <= lagsse_m1) {
        val e1 = (DenseVector(c) + (DenseVector(r) - DenseVector(c)) * 0.5).toArray
        val lagsse_e1 = -lagsse4optimize(e1(0), DenseVector(e1.drop(1)))
        if (lagsse_e1 <= lagsse_r) {
          ord_m1_change = e1
        } else {
          val ord_1m1vec = ord_1m1Arr.map(t => DenseVector(t))
          val v = ord_1m1vec.map(t => t + (t - DenseVector(ord_0)) * 0.5)
          ord_Arr.clear()
          ord_Arr += ord_0
          val v_arr = v.map(t => t.toArray).toArray
          for (i <- 0 until v_arr.length) {
            ord_Arr += v_arr(i)
          }
          flag_A10 = true
        }
      } else if (lagsse_r > lagsse_m1) {
        val e2 = (DenseVector(c) + (DenseVector(ord_m1) - DenseVector(c)) * 0.5).toArray
        val lagsse_e2 = -lagsse4optimize(e2(0), DenseVector(e2.drop(1)))
        if (lagsse_e2 <= lagsse_m1) {
          ord_m1_change = e2
        } else {
          val ord_1m1vec = ord_1m1Arr.map(t => DenseVector(t))
          val v = ord_1m1vec.map(t => t + (t - DenseVector(ord_0)) * 0.5)
          ord_Arr.clear()
          ord_Arr += ord_0
          val v_arr=v.map(t=>t.toArray).toArray
          for (i <- 0 until v_arr.length) {
            ord_Arr += v_arr(i)
          }
          flag_A10 = true
        }
      } else {
        val ord_1m1vec = ord_1m1Arr.map(t => DenseVector(t))
        val v = ord_1m1vec.map(t => t + (t - DenseVector(ord_0)) * 0.5)
        ord_Arr.clear()
        ord_Arr += ord_0
        val v_arr = v.map(t => t.toArray).toArray
        for (i <- 0 until v_arr.length) {
          ord_Arr += v_arr(i)
        }
        flag_A10 = true
      }
      // 一般情况下需要更新m+1，把m+1放入arr种
      if (!flag_A10) {
        ord_Arr += ord_m1_change
      }
      //      ord_Arr.map(t => t.foreach(println))
//      println(flag_A10)
      //更新sse的值
      arr_lagsse = ord_Arr.toArray.map(t => -lagsse4optimize(t(0), DenseVector(t.drop(1))))
      //      arr_lagsse.foreach(println)
//      println(lagsse_r <= lagsse_0 ,lagsse_r > lagsse_0 && lagsse_r <= lagsse_m, lagsse_r > lagsse_m && lagsse_r <= lagsse_m1, lagsse_r > lagsse_m1)
      iter += 1
    }
    println("-------result--------")
    ord_Arr.map(t => t.foreach(println))
    println("---input---")
    optParameter.foreach(println)
  }

  def nm_gravityCenter(calArr: Array[Array[Double]]): Array[Double] = {
    val flat = calArr.flatMap(t => t.zipWithIndex)
    //    flat.foreach(println)
    val re = new Array[Double](calArr(0).length)
    for (i <- 0 until re.length) {
      val tmp = flat.filter(t => t._2 == i).map(t => t._1)
      re(i) = tmp.sum / tmp.length
    }
    //    re.foreach(println)
    re
  }

}
