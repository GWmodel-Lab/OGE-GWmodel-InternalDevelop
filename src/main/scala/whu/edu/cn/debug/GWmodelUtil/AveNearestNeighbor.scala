package whu.edu.cn.debug.GWmodelUtil

import scala.collection.immutable.List
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{abs, max, min, pow, sqrt}
import scala.reflect.ClassTag
import org.apache.spark.{SparkConf, SparkContext}  //单独引用函数
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.stat.Statistics
import breeze.numerics._
import breeze.linalg.{Vector, DenseVector, Matrix , DenseMatrix}
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._
import whu.edu.cn.oge.Feature._

object AveNearestNeighbor {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf) //创建RDD
    val shpPath: String = "testdata\\LNHP100.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
    val testshp = ShapeFileUtil.readShp(sc, shpPath, ShapeFileUtil.DEF_ENCODE) //或者直接utf-8

    AveNearestNeighbor(sc,testshp)

  }

  //满足按距离排序后除0以外的最小距离
  def dismin(Arr1: Array[Double]) : Double = {
      Arr1.sorted.filter(_>0).apply(0)
  }

  //要素的最小外接矩阵面积
  def ExRectangularArea(testshp: RDD[(String, (Geometry, Map[String, Any]))]) : Double = {
    val coorxy : Array[(Double, Double)] = getCoorXY(testshp: RDD[(String, (Geometry, Map[String, Any]))])
    val coorx_min : Double = coorxy.map{case t => t._1}.min
    val coory_min : Double = coorxy.map{case t => t._2}.min
    val coorx_max : Double = coorxy.map{case t => t._1}.max
    val coory_max : Double = coorxy.map{case t => t._2}.max
    val Area : Double = (coorx_max - coorx_min) * (coory_max - coory_min)
    Area
  }

  //平均最近邻指数算子
  def AveNearestNeighbor(sc: SparkContext,testshp: RDD[(String, (Geometry, Map[String, Any]))]): Unit={
    val DisArray = getRDDDistRDD(sc,testshp)  //getRDDDistRDD函数构造RDD的距离矩阵
    //DisArray.collect().map(t => println(dismin(t)))
    val DisSum = DisArray.collect().map(t => dismin(t)).sum  //t指每一行，取出RDD矩阵每行最小距离，求和
    val RDDsize =  DisArray.collect().size  //RDD要素个数，同length
    val A  = ExRectangularArea(testshp)
    val Do = DisSum/RDDsize  //观测平均距离（Observed Mean Distance）
    val De = 0.5/(sqrt(RDDsize/A))  //预期平均距离 A为研究区域面积，要素的外接矩形
    val ANN = Do/De  //平均最近邻指数  ANN>1离散  ANN<1聚集
    println(ANN)
  }

}
