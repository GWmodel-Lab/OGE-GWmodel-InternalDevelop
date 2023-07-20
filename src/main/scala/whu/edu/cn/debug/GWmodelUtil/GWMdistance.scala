package whu.edu.cn.debug.GWmodelUtil

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import scala.math.{abs, max, min, pow, sqrt}
import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.spark.mllib.stat._
import org.apache.spark.mllib.linalg._

object GWMdistance {

  def getCoorXY(featureRDD: RDD[(String, (Geometry, Map[String, Any]))]): Array[(Double, Double)] = {
    val coorxy = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y)
    }).collect()
    coorxy
  }

  def pointDist(x1: (Double, Double), x2: (Double, Double)): Double = {
    val dis = sqrt(pow(x1._1 - x2._1, 2) + pow(x1._2 - x2._2, 2))
    dis
  }

  def arrDist(Arr1: Array[(Double, Double)], Arr2: Array[(Double, Double)]): Array[Double] = {
    var arrbuf: ArrayBuffer[Array[Double]] = ArrayBuffer()
    for (i <- 0 until Arr2.length) {
      //      var targeti = targetArr(i)
      arrbuf += ( Arr1.map(t => pointDist(t,Arr2(i))) )
    }
    arrbuf.flatMap(t=>t).toArray
  }

  def rddcoorDist(sourceCoor: RDD[Coordinate], targetCoor: Coordinate): RDD[Double] = {
    val RDDdist = sourceCoor.map(t => t.distance(targetCoor))
    RDDdist
  }

  def arrcoorDist(sourceCoor: RDD[Coordinate], targetCoor: Coordinate): Array[Double] = {
    val arrdist = sourceCoor.map(t => t.distance(targetCoor)).collect()
    arrdist
  }

  def arrbufDist(sourceCoor: RDD[Coordinate], targetCoor: RDD[Coordinate]): ArrayBuffer[Array[Double]] = {
    val targetArr = targetCoor.collect()
    var arrbufdist: ArrayBuffer[Array[Double]] = ArrayBuffer()
    for (i <- 0 until targetArr.length) {
      //      var targeti = targetArr(i)
      arrbufdist += arrcoorDist(sourceCoor, targetArr(i))
    }
    arrbufdist
  }

  def rddarrDist(sc: SparkContext, RDDX1: RDD[(String, (Geometry, Map[String, Any]))], RDDX2: RDD[(String, (Geometry, Map[String, Any]))]): RDD[Array[Double]] = {
    val RDDcoor1 = RDDX1.map(t => t._2._1.getCoordinate)
    val RDDcoor2 = RDDX2.map(t => t._2._1.getCoordinate)
    val arrbuf = arrbufDist(RDDcoor1, RDDcoor2)
    val RDDarr = sc.makeRDD(arrbuf)
    RDDarr
  }

  def arrSelfDist(inputshp: RDD[(String, (Geometry, Map[String, Any]))]): Array[Double] = {
    val coorshp = inputshp.map(t => t._2._1.getCoordinate)
    val arrbuf = arrbufDist(coorshp, coorshp).flatMap(t => t).toArray
    arrbuf
  }

  def dmatRddDist(sc: SparkContext, RDDX1: RDD[(String, (Geometry, Map[String, Any]))], RDDX2: RDD[(String, (Geometry, Map[String, Any]))]): DenseMatrix ={
    val RDDarr=rddarrDist(sc,RDDX1,RDDX2)
    val arrflat = RDDarr.flatMap(t => t).collect()
    val dmat = new DenseMatrix(RDDX1.collect().length, RDDX2.collect().length, arrflat)
    dmat
  }

  def dmatArrDist(Arr1: Array[(Double, Double)], Arr2: Array[(Double, Double)]): DenseMatrix = {
    val arrdist = arrDist(Arr1, Arr2)
    val dmat= new DenseMatrix(Arr1.length, Arr2.length, arrdist)
    dmat
  }

}