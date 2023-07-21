package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.{DenseMatrix, DenseVector, Matrix, Vector}
import breeze.numerics._
import geotrellis.raster.mapalgebra.focal.Kernel
import org.apache.spark.rdd.RDD
import org.sparkproject.dmg.pmml.False

import scala.collection.mutable.ArrayBuffer
//import cern.colt.matrix._
import org.apache.spark.mllib.linalg._

object GWMspatialweight {

//  object Kernal extends Enumeration {
//    val gaussian, exponential, bisquare, tricube, boxcar = Value
//  }


  /**
   * 对单个距离向量进行权重向量求解
   *
   * @param dist \~english Distance vector \~chinese 距离向量
   * @param bw   \~english Bandwidth size \~chinese 带宽大小
   * @param kernel   \~english Kernel function, default is "gaussian" \~chinese 核函数，默认为高斯核函数
   * @param adaptive   \~english bandwidth type: adaptive(true) or fixed(false, default) \~chinese 带宽类型，可变带宽为true，固定带宽为false，默认为固定带宽
   * @return \~english Weight value \~chinese 权重向量
   */
  def spatialweightSingle(dist: DenseVector[Double], bw:Double, kernel: String="gaussian", adaptive: Boolean=false ): DenseVector[Double] = {
    var weight :DenseVector[Double] =DenseVector.zeros(dist.length)
    if (adaptive == false){
      kernel match {
        case "gaussian" => weight = gaussianKernelFunction(dist, bw)
        case "exponential"=> weight = exponentialKernelFunction(dist, bw)
        case "bisquare"=> weight = bisquareKernelFunction(dist, bw)
        case "tricube"=> weight = tricubeKernelFunction(dist, bw)
        case "boxcar"=> weight = boxcarKernelFunction(dist, bw)
        case _=> throw new IllegalArgumentException("Illegal Argument of kernal")
      }
    }else if (adaptive == true){
      val fbw = fixedwithadaptive(dist,bw.toInt)
      kernel match {
        case "gaussian" => weight = gaussianKernelFunction(dist, fbw)
        case "exponential" => weight = exponentialKernelFunction(dist, fbw)
        case "bisquare" => weight = bisquareKernelFunction(dist, fbw)
        case "tricube" => weight = tricubeKernelFunction(dist, fbw)
        case "boxcar" => weight = boxcarKernelFunction(dist, fbw)
        case _=> throw new IllegalArgumentException("Illegal Argument of kernal")
      }
    }else{
      throw new IllegalArgumentException("Illegal Argument of adaptive")
    }
    weight
  }

  def spatialweightRDD(distRDD: RDD[Array[Double]], bw:Double, kernel: String="gaussian", adaptive: Boolean=false): RDD[DenseVector[Double]]= {
    val RDDdvec=distRDD.map(t=>Array2DenseVector(t))
    RDDdvec.map(t=>spatialweightSingle(t,bw, kernel, adaptive))
  }

  def Array2DenseVector(inputArr: Array[Double]): DenseVector[Double] ={
    val dvec: DenseVector[Double] = new DenseVector(inputArr)
    dvec
  }

  def DenseVector2Array(inputDvec: DenseVector[Double]): Array[Double]={
    val arr=inputDvec.toArray
    arr
  }

  /**
   *  \~english Gaussian kernel function. \~chinese Gaussian 核函数。
   * @param dist \~english Distance vector \~chinese 距离向量
   * @param bw   \~english Bandwidth size (its unit is equal to that of distance vector) \~chinese 带宽大小（和距离向量的单位相同）
   * @return \~english Weight value \~chinese 权重值
   */
  def gaussianKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
//    exp((dist % dist) / ((-2.0) * (bw * bw)))
    exp(-0.5*((dist / bw) * (dist / bw)))
  }

  def exponentialKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
    exp(-dist / bw);
  }

  //bisquare (1-(vdist/bw)^2)^2
  def bisquareKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
    val d2_d_b2:DenseVector[Double] = 1.0 - (dist / bw) * (dist / bw);
//    val arr_re: Array[Double] = dist.toArray.filter(_<bw)
//    val dist_bw = DenseVector(dist.toArray.filter(_<bw))//这样会筛选出重新组成一个数组
    val weight = (d2_d_b2 * d2_d_b2)
    filterwithBw(dist,weight,bw)
  }

  def tricubeKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
    val d3_d_b3:DenseVector[Double] = 1.0 - (dist * dist * dist) / (bw * bw * bw);
    val weight = (d3_d_b3 * d3_d_b3 * d3_d_b3)
    filterwithBw(dist,weight,bw)
  }

  def boxcarKernelFunction(dist: DenseVector[Double], bw:Double): DenseVector[Double] ={
    val weight :DenseVector[Double] =DenseVector.ones(dist.length)
    filterwithBw(dist,weight,bw)
  }

  def filterwithBw(dist:DenseVector[Double],weight:DenseVector[Double], bw:Double): DenseVector[Double]={
    for(i <-0 until dist.length){
      if (dist(i) >= bw ){
        weight(i) = 0.0
      }
    }
    weight
  }
  def fixedwithadaptive(dist: DenseVector[Double], abw:Int): Double = {
    val distcopy=dist.copy.toArray.sorted
    var fbw=distcopy.max
    if (abw < distcopy.length) {
      fbw = distcopy(abw+1)
    }
    else{
      println("Error bandwidth, biggest bandwidth has been set")
    }
    fbw
  }
}
