package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.{Vector, DenseVector, Matrix , DenseMatrix}

import breeze.numerics._
//import cern.colt.matrix._
import org.apache.spark.mllib.linalg._

object GWMspatialweight {

  /**
   * @brief \~english Gaussian kernel function. \~chinese Gaussian 核函数。
   * @param dist \~english Distance vector \~chinese 距离向量
   * @param bw   \~english Bandwidth size (its unit is equal to that of distance vector) \~chinese 带宽大小（和距离向量的单位相同）
   * @return \~english Weight value \~chinese 权重值
   */
  def GaussianKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
    exp((dist % dist) / ((-2.0) * (bw * bw)))
  }

  def ExponentialKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
    exp(-dist / bw);
  }

  def BisquareKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
    val d2_d_b2:DenseVector[Double] = 1.0 - (dist % dist) / (bw * bw);
//    val arr_re: Array[Double] = dist.toArray.filter(_<bw)
    val dist_bw = DenseVector(dist.toArray.filter(_<bw))
    dist_bw % (d2_d_b2 % d2_d_b2)
  }

  def TricubeKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
    val d3_d_b3:DenseVector[Double] = 1.0 - (dist % dist % dist) / (bw * bw * bw);
//    val arr_re: Array[Double] = dist.toArray.filter(_<bw)
    val dist_bw = DenseVector(dist.toArray.filter(_<bw))
    dist_bw % (d3_d_b3 % d3_d_b3 % d3_d_b3)
  }


  def BoxcarKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
//    val arr_re=dist.toArray.filter(_<bw)
    val dist_bw=DenseVector(dist.toArray.filter(_<bw))
    val right_re:DenseVector[Double] =DenseVector.ones(dist.length)
    dist_bw % right_re
  }
}
