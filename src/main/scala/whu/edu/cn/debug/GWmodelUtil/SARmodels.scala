package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import scala.collection.mutable.{ArrayBuffer, Map}

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

//后续改写成抽象类
class SARmodels {

  protected var _X: Array[DenseVector[Double]]=_
  protected var _Y: DenseVector[Double]=_
  protected var geom: RDD[Geometry]=_
  var spweight_dvec: Array[DenseVector[Double]]=_
  var spweight_dmat: DenseMatrix[Double]=_

  def SARmodels(){

  }

  def init(inputRDD: RDD[(String, (Geometry, Map[String, Any]))]): Unit = {
    geom=getGeometry(inputRDD)
  }

  protected def setX(x: Array[DenseVector[Double]])={
    _X = x
  }

  protected def setY(y: Array[Double]) = {
    _Y = DenseVector(y)
  }

  protected def getdistance(): Array[Array[Double]] = {
    val coords = geom.map(t => t.getCoordinate)
    getCoorDistArrbuf(coords, coords).toArray
  }

  def fit()={
//    _x.foreach(println)
//    _y.foreach(println)
    println("created")
  }

  def setcoords(lat:Array[Double],lon:Array[Double])={
    val geomcopy=geom.zipWithIndex()
    geomcopy.map(t=>{
      t._1.getCoordinate.x = lat(t._2.toInt)
      t._1.getCoordinate.y = lon(t._2.toInt)
    })
    geom=geomcopy.map(t=>t._1)
  }

  def setweight(neighbor:Boolean=true, k:Double=0)={
    if(neighbor && !geom.isEmpty()) {
      val nb_bool = getNeighborBool(geom)
      spweight_dvec = boolNeighborWeight(nb_bool).map(t => t * (t / t.sum)).collect()
    }else if(neighbor==false && !geom.isEmpty() && k>=0){
      val dist=getdistance().map(t => Array2DenseVector(t))
      spweight_dvec = dist.map(t => getSpatialweightSingle(t, k, kernel = "boxcar", adaptive = true))
    }
    spweight_dmat = DenseMatrix.create(rows=spweight_dvec(0).length,cols=spweight_dvec.length,data = spweight_dvec.flatMap(t=>t.toArray))
  }

}
