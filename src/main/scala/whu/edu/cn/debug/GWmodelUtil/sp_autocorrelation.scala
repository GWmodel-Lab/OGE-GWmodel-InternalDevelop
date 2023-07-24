package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import scala.collection.mutable.Map
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

object sp_autocorrelation {

  def globalMoranI(featRDD: RDD[(String, (Geometry, Map[String, Any]))], property: String): Double={
    val nb_weight=getNeighborWeight(featRDD)
    val sum_weight=sumWeight(nb_weight)
    val arr=featRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect()
    val arrmul=localSelfdiff(arr).map(t=>{
      var mul = 0.0
      for (i <- 0 until arr.length) {
        mul = t * localSelfdiff(arr)(i)
      }
      mul
    })
    val rightup=nb_weight.flatMap(t=>((t * DenseVector(arrmul)).toArray)).collect().sum
    val rightdn=localSelfdiff(arr).map(t=>t*t).sum
    arr.length/sum_weight*rightup/rightdn
  }

  def arr2multi(arr1: Array[Double], arr2:Array[Double]): DenseVector[Double]={
    val re=new Array[Double](arr1.length)
    for (i <- 0 until arr1.length if arr1.length==arr2.length){
        re(i)=arr1(i)*arr2(i)
      }
    DenseVector(re)
  }

  def localSelfdiff(arr:Array[Double]):Array[Double]={
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
