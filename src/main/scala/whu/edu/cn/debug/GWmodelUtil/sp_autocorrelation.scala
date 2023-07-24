package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import scala.math.pow
import scala.collection.mutable.Map
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

object sp_autocorrelation {

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

  def localMoranI(featRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String)={

  }

  def arrdvec2multi(arr1:Array[DenseVector[Double]], arr2:Array[DenseVector[Double]]): Array[DenseVector[Double]]={
    val dmat1=DenseMatrix(arr1)
    val dmat2=DenseMatrix(arr2)
    val re=dmat1 * dmat2
    re.toArray
  }

  def arr2multi(arr1: Array[Double], arr2:Array[Double]): DenseVector[Double]={
    val re=new Array[Double](arr1.length)
    for (i <- 0 until arr1.length if arr1.length==arr2.length){
        re(i)=arr1(i)*arr2(i)
      }
    DenseVector(re)
  }

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
