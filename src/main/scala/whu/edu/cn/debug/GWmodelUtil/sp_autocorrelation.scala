package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import scala.math.pow
import scala.collection.mutable.Map
import java.text.SimpleDateFormat
import breeze.plot._

import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

object sp_autocorrelation {

  /**
   * 输入RDD直接计算全局莫兰指数
   *
   * @param featRDD   RDD
   * @param property  要计算的属性，String
   * @return  （全局莫兰指数，峰度）(Double,Double)形式
   */
  def globalMoranI(featRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): (Double,Double)={
    val nb_weight=getNeighborWeight(featRDD)
    val sum_weight=sumWeight(nb_weight)
    val arr=featRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect()
    val arr_mean=meandiff(arr)
    val arr_mul = arr_mean.map(t => {
      val re = new Array[Double](arr_mean.length)
      for (i <- 0 until arr_mean.length) {
        re(i) = t * arr_mean(i)
      }
      DenseVector(re)
    })
    val weight_m_arr=arrdvec2multi(nb_weight.collect(),arr_mul)
    val rightup=weight_m_arr.map(t=>t.sum).sum
    val rightdn=arr_mean.map(t=>t*t).sum
    val moran_i=arr.length/sum_weight*rightup/rightdn
    val kurtosis= (arr.length * arr_mean.map(t=>pow(t,4)).sum) / pow(rightdn,2)
    (moran_i,kurtosis)
  }

  def globalMoranItest(featRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String)={
//    remain to be coded
  }

  /**
   * 输入RDD直接计算局部莫兰指数
   *
   * @param featRDD  RDD
   * @param property 要计算的属性，String
   * @return （局部莫兰指数，预测值）Tuple2[Array,Array]形式
   */
  def localMoranI(featRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String, plot: Boolean = false): Tuple2[Array[Double],Array[Double]] = {
    val nb_weight = getNeighborWeight(featRDD)
    val arr = featRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect()
    val arr_mean = meandiff(arr)
    val arr_mul = arr_mean.map(t => {
      val re = arr_mean.clone()
      DenseVector(re)
    })
    val weight_m_arr = arrdvec2multi(nb_weight.collect(), arr_mul)
    val rightup = weight_m_arr.map(t => t.sum)
    val leftdn = arr_mean.map(t => t * t).sum / (arr_mean.length)
//    > m2 <- sum(z * z) / n
//    > - (z ^ 2 * Wi) / ((n - 1) * m2)
    val dvec_mean=DenseVector(arr_mean)
    val expectation = - dvec_mean*dvec_mean/((arr_mean.length-1)*(dvec_mean*dvec_mean).sum/arr_mean.length)
    val local_moranI = (dvec_mean / leftdn * DenseVector(rightup)).toArray
    if(plot==true){
      plotmoran(featRDD,local_moranI)
    }
    (local_moranI,expectation.toArray)
  }

  def localMoranItest(featRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String) = {
    //    remain to be coded
  }

  def plotmoran(featRDD: RDD[(String, (Geometry, Map[String, Any]))], resultArr: Array[Double])={
    val x=featRDD.map(t=>t._2._1.getCoordinate.x).collect()
    val y=resultArr
    val f = Figure()
    val p = f.subplot(0)
    p += plot(x, y, '.')
    p.xlabel = "x axis"
    p.ylabel = "morani"
  }

  def arrdvec2multi(arr1:Array[DenseVector[Double]], arr2:Array[DenseVector[Double]]): Array[DenseVector[Double]]={
    val dmat1=DenseMatrix(arr1)
    val dmat2=DenseMatrix(arr2)
    val re=dmat1 * dmat2
    re.toArray
  }

//  def arr2multi(arr1: Array[Double], arr2:Array[Double]): Array[Double]={
//    val re=new Array[Double](arr1.length)
//    for (i <- 0 until arr1.length if arr1.length==arr2.length){
//        re(i)=arr1(i)*arr2(i)
//      }
//    re
//  }

  def meandiff(arr:Array[Double]):Array[Double]={
    val ave=arr.sum/arr.length
    arr.map(t=>t-ave)
  }
  def sumWeight(weightRDD:RDD[DenseVector[Double]]): Double={
    weightRDD.map(t=>t.sum).sum()
  }

//  def getAverage(featRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): Double={
//    val arr=featRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect()
//    arr.sum / arr.length
//  }

}
