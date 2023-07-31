package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.{DenseMatrix, DenseVector, inv, linspace}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.math.{exp, pow, sqrt}
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
  def globalMoranI(featRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String, test:Boolean=false): (Double,Double)={
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
    val n=arr.length
    val moran_i=n/sum_weight*rightup/rightdn
    val kurtosis= (n * arr_mean.map(t=>pow(t,4)).sum) / pow(rightdn,2)
    if(test) {
      val E_I = -1.0 / (n - 1)
      val S_1 = 0.5 * nb_weight.map(t => t.map(t => t * 2 * t * 2).sum).sum()
      val S_2 = nb_weight.map(t => t.sum * 2).sum()
      val E_A = n * ((n * n - 3 * n + 3) * S_1) - n * S_2 + 3 * sum_weight * sum_weight
      val E_B = (arr_mean.map(t => t * t * t * t).sum / (rightdn * rightdn)) * ((n * n - n) * S_1 - 2 * n * S_2 + 6 * sum_weight * sum_weight)
      val E_C = (n - 1) * (n - 2) * (n - 3) * sum_weight * sum_weight
      val V_I = (E_A - E_B) / E_C - pow(E_I, 2)
      val Z_I = (moran_i - E_I) / (sqrt(V_I))
      val Pvalue = (1 / (sqrt(V_I) * sqrt(2 * 3.14159265))) * exp(-pow(Z_I - E_I, 2) / (2 * V_I))
      if(Pvalue < 0.01){
        println(s"Z-Score is: $Z_I, p < 0.01")
      }
      println(E_I, V_I, Z_I, Pvalue)
    }
    (moran_i,kurtosis)
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
    val dvec_mean=DenseVector(arr_mean)

    val n  = arr.length //加个判断变换
    val s2 = arr_mean.map(t => t * t).sum / n
    val lz = DenseVector(rightup)
    val z  = dvec_mean
    val m2 = ((dvec_mean*dvec_mean).sum/ n )
    val expectation = - z*z / ((n-1)* m2 )
    val local_moranI = (z / s2 * lz)
    if(plot==true){
      plotmoran(arr,nb_weight)
    }
//    b2 <- (sum(z^4, na.rm = NAOK)/n)/(s2^2)
    val b2=((z*z*z*z).sum/ n)/ (s2*s2)
    val wi2=DenseVector(nb_weight.map(t=>(t*t).sum).collect())
//    res[,3] <- ((z / m2) ^ 2 * (n / (n - 2)) * (Wi2 - (Wi ^ 2 / (n - 1))) * (m2 - (z ^ 2 / (n - 1))))
    val var_I=(z/m2)*(z/m2) * (n/(n-2.0)) * (wi2 - (wi2 * wi2 / (n-1.0))) * (m2 - (z * z / (n - 1.0)))
    val Z_I =( local_moranI- expectation) / var_I.map(t=>sqrt(t))
    println(var_I)
    println(Z_I)

    (local_moranI.toArray,expectation.toArray)
  }

  def plotmoran(x: Array[Double], w: RDD[DenseVector[Double]])={
    val xx=x
    val wx=w.map(t=> t dot DenseVector(x)).collect()
    val f = Figure()
    val p = f.subplot(0)
    p += plot(xx, wx, '+')
    val xxmean=DenseVector.ones[Double](x.length) :*= (xx.sum/xx.length)
    val wxmean=DenseVector.ones[Double](x.length) :*= (wx.sum/wx.length)
    val xxy=linspace(wx.min-2,wx.max+2,x.length)
    val wxy=linspace(xx.min-2,xx.max+2,x.length)
//    val xt=DenseMatrix(DenseVector.ones[Double](x.length),DenseVector(x))
//    val yxt=inv(xt) * DenseVector(wx)
//    val y=DenseMatrix(wxy,DenseVector.ones[Double](x.length)).t * yxt
    p.xlim = (xx.min-2,xx.max+2)
    p.ylim = (wx.min-2,wx.max+2)
//    p += plot(wxy,y)
    p += plot(xxmean,xxy,lines = false,shapes=true, style = '.', colorcode="[0,0,0]")
    p += plot(wxy,wxmean,lines = false,shapes=true, style = '.', colorcode="[0,0,0]")
    p.xlabel = "x"
    p.ylabel = "wx"
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
