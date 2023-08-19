package whu.edu.cn.debug.GWmodelUtil.Utils

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{pow, sqrt}

object GWMdistance {

  /**
   * 输入RDD，获得Array[(Double, Double)]
   */
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

  /**
   * 输入两个Array[(Double, Double)]，输出他们之间的距离，结果为Array
   *
   * @param Arr1 Array[(Double, Double)]的形式
   * @param Arr2 Array[(Double, Double)]的形式
   * @return Array[Double] 结果Array应按顺序存储了Arr1第i个坐标和Arr2每个坐标的距离
   */
  def arrayDist(Arr1: Array[(Double, Double)], Arr2: Array[(Double, Double)]): Array[Double] = {
    var arrbuf: ArrayBuffer[Array[Double]] = ArrayBuffer()
    for (i <- 0 until Arr1.length) {
      //      var targeti = targetArr(i)
      arrbuf += ( Arr2.map(t => pointDist(t,Arr1(i))) )
    }
    arrbuf.flatMap(t=>t).toArray
  }

  def rddcoorDist(sourceCoor: Coordinate, targetCoor: RDD[Coordinate]): RDD[Double] = {
    val RDDdist = targetCoor.map(t => t.distance(sourceCoor))
    RDDdist
  }

  /**
   * 输入源数据为Coordinate，输出与计算目标RDD[Coordinate]之间每一个的距离，输出结果为Array
   *
   * @param sourceCoor Coordinate的形式
   * @param targetCoor RDD[Coordinate]的形式
   * @return Array[Double] 结果Array应按顺序存储了source和target每个元素的距离
   */
  def arrcoorDist(sourceCoor: Coordinate, targetCoor: RDD[Coordinate]): Array[Double] = {
    val arrdist = targetCoor.map(t => t.distance(sourceCoor)).collect()
    arrdist
  }

  /**
   * 输入两个RDD[Coordinate]获得距离，返回ArrayBuffer
   *
   * @param sourceCoor RDD[Coordinate]的形式
   * @param targetCoor RDD[Coordinate]的形式
   * @return ArrayBuffer[Array[Double]] 结果中每个Array应按顺序存储了source第i个元素和target所有元素的距离
   *         ArrayBuffer形式可以进行后续变换
   */
  def getCoorDistArrbuf(sourceCoor: RDD[Coordinate], targetCoor: RDD[Coordinate]): ArrayBuffer[Array[Double]] = {
    val sourceArr = sourceCoor.collect()
    var arrbufdist: ArrayBuffer[Array[Double]] = ArrayBuffer()
    for (i <- 0 until sourceArr.length) {
      //      var targeti = targetArr(i)
      arrbufdist += arrcoorDist(sourceArr(i), targetCoor)
    }
    arrbufdist
  }

  /**
   * 输入一个RDD以及SparkContext获得RDD自身的距离，返回RDD
   *
   * @param sc    SparkContext，需要在计算中生成RDD
   * @param inputRDD 项目默认矢量RDD的形式
   * @return 输入RDD的距离矩阵，以RDD[Array[Double]]的形式
   */
  def getRDDDistRDD(sc: SparkContext, inputRDD: RDD[(String, (Geometry, Map[String, Any]))]): RDD[Array[Double]] = {
    val RDDcoor = inputRDD.map(t => t._2._1.getCentroid.getCoordinate)
    val arrbuf = getCoorDistArrbuf(RDDcoor, RDDcoor)
    val RDDarr = sc.makeRDD(arrbuf)
    RDDarr
  }

  /**
   * 输入两个RDD以及SparkContext获得距离，返回RDD
   *
   * @param sc    SparkContext，需要在计算中生成RDD
   * @param RDDX1 项目默认矢量RDD的形式
   * @param RDDX2 项目默认矢量RDD的形式
   * @return RDD[Array[Double]] 结果中每个Array应按顺序存储了RDDX1第i个元素和RDDX2所有元素的距离
   */
  def get2RDDDistRDD(sc: SparkContext, RDDX1: RDD[(String, (Geometry, Map[String, Any]))], RDDX2: RDD[(String, (Geometry, Map[String, Any]))]): RDD[Array[Double]] = {
    val RDDcoor1 = RDDX1.map(t => t._2._1.getCentroid.getCoordinate)
    val RDDcoor2 = RDDX2.map(t => t._2._1.getCentroid.getCoordinate)
    val arrbuf = getCoorDistArrbuf(RDDcoor1, RDDcoor2)
    val RDDarr = sc.makeRDD(arrbuf)
    RDDarr
  }

  def getSelfDistArr(inputshp: RDD[(String, (Geometry, Map[String, Any]))]): Array[Double] = {
    val coorshp = inputshp.map(t => t._2._1.getCoordinate)
    val arr = getCoorDistArrbuf(coorshp, coorshp).flatMap(t => t).toArray
    arr
  }

  /**
   * 输入两个RDD以及SparkContext获得Dmat
   *
   * @param sc SparkContext，引入是因为需要在计算中生成RDD
   * @param RDDX1 项目默认矢量RDD的形式
   * @param RDDX2 项目默认矢量RDD的形式
   * @return Dmat结果为转置后的结果，转置的原因是，Dmat默认按列存储。
   *         输出的第一行为RDDX1第一个元素和RDDX2所有元素的距离。
   *         如有后续计算需要，可以考虑将转置取消
   */
  def get2RDDDistDmat(sc: SparkContext, RDDX1: RDD[(String, (Geometry, Map[String, Any]))], RDDX2: RDD[(String, (Geometry, Map[String, Any]))]): DenseMatrix ={
    val RDDarr=get2RDDDistRDD(sc,RDDX1,RDDX2)
    val arrflat = RDDarr.flatMap(t => t).collect()
    val dmat = new DenseMatrix(RDDX1.collect().length, RDDX2.collect().length, arrflat)
    dmat.transpose
  }

  /**
   * 输入两个Array获得Dmat
   *
   * @param Arr1 需要是(Double, Double)的形式
   * @param Arr2 需要是(Double, Double)的形式
   * @return Dmat结果为转置后的结果，转置的原因是，Dmat默认按列存储。
   *         输出的第一行为Arr1第一个元素和Arr2所有元素的距离。
   *         如有后续计算需要，可以考虑将转置取消
   */
  def getArrDistDmat(Arr1: Array[(Double, Double)], Arr2: Array[(Double, Double)]): DenseMatrix = {
    val arrdist = arrayDist(Arr1, Arr2)
    val dmat= new DenseMatrix(Arr2.length, Arr1.length, arrdist)
    dmat.transpose
  }

}