package whu.edu.cn.debug.GWmodelUtil

import geotrellis.vector.MultiPolygon
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry, LineString, Point}
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil._

import java.awt.Polygon
import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{abs, max, min, pow, sqrt}
import scala.reflect.ClassTag
//import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.stat.Statistics
import breeze.numerics._
import breeze.linalg.{Vector, DenseVector, Matrix , DenseMatrix}
import scala.collection.JavaConverters._

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._
import whu.edu.cn.debug.GWmodelUtil.sp_autocorrelation._

object testRun {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    val shpPath: String = "testdata\\MississippiHR.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
    val shpfile = readShp(sc,shpPath,DEF_ENCODE)//或者直接utf-8
//    val geom=println(getGeometryType(shpfile))

    val globali = globalMoranI(shpfile, "HR60")
    println(s"global Moran's I is: ${globali._1}")

    val locali=localMoranI(shpfile,"HR60")
    println("-----------local moran's I--------------")
    locali._1.foreach(println)

//    val result1=writeRDD(sc,shpfile,locali._1,"moran_i")
//    val result2=writeRDD(sc,result1,locali._2,"expect")
//    val outputpath="testdata\\MississippiMoranI.shp"
//    writeshpfile(result2,outputpath)

  }

  /**
   * 输入写出的RDD和路径，写出shpfile。添加了一定的类型判断，没有测试
   *
   * @param outputshpRDD 要写出的RDD
   * @param outputshpPath 要写出的路径
   */
  def writeshpfile(outputshpRDD: RDD[(String, (Geometry, Map[String, Any]))], outputshpPath:String)={
    val geom=getGeometryType(outputshpRDD)
    geom match {
      case "MultiPolygon" => createShp(outputshpPath, "utf-8", classOf[MultiPolygon], outputshpRDD.map(t => {
        t._2._2 + (DEF_GEOM_KEY -> t._2._1)
      }).collect().map(_.asJava).toList.asJava)
      case "Point" => createShp(outputshpPath, "utf-8", classOf[Point], outputshpRDD.map(t => {
        t._2._2 + (DEF_GEOM_KEY -> t._2._1)
      }).collect().map(_.asJava).toList.asJava)
      case "LineString" => createShp(outputshpPath, "utf-8", classOf[LineString], outputshpRDD.map(t => {
        t._2._2 + (DEF_GEOM_KEY -> t._2._1)
      }).collect().map(_.asJava).toList.asJava)
      case _ => throw new IllegalArgumentException("can not modified geometry type, please retry")
    }
    println(s"shpfile written successfully in $outputshpPath")
  }

  /**
   * 输入要添加的属性数据和RDD，输出RDD
   *
   * @param sc  SparkContext
   * @param shpRDD 源RDD
   * @param writeArray  要写入的属性数据，Array形式
   * @param propertyName  属性名，String类型，需要少于10个字符
   * @return  RDD
   */
  def writeRDD(sc: SparkContext, shpRDD: RDD[(String, (Geometry, Map[String, Any]))], writeArray:Array[Double], propertyName:String): RDD[(String, (Geometry, Map[String, Any]))] ={
    if(propertyName.length >= 10){
      throw new IllegalArgumentException("the length of property name must not longer than 10!!")
    }
    val shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1._2._2 += (propertyName -> writeArray(t._2))
    })
    sc.makeRDD(shpRDDidx.map(t => t._1))
  }

  def printArrArr[T: ClassTag](arrarr: Array[Array[T]]) = {
    val arrvec=arrarr.map(t=>t.toVector)
    arrvec.foreach(println)
  }

  def testcorr(testshp: RDD[(String, (Geometry, Map[String, Any]))]) : Unit={
    val time0: Long = System.currentTimeMillis()
    val list1:List[Any] = Feature.get(testshp,"PURCHASE")//FLOORSZ,PROF
    val list2:List[Any] = Feature.getNumber(testshp,"FLOORSZ")
//    println(list1)
//    println(list2)
    val lst1:List[Double] = list1.collect({ case (i: String) => (i.toDouble) })
//    //val lst2:List[Double]=list2.collect({ case (i: String) => (i.toDouble) })
    val lst2:List[Double] = list2.asInstanceOf[List[Double]]
    val corr1 = corr2list(lst1,lst2)
    val listtimeused= System.currentTimeMillis()-time0
    println(s"time used is: $listtimeused")

    val coor2 = corr2ml(testshp,"PURCHASE","FLOORSZ")

  }

  def corr2list(lst1: List[Double], lst2: List[Double]): Double = {
    val sum1 = lst1.sum
    val sum2 = lst2.sum
    val square_sum1 = lst1.map(x => x * x).sum
    val square_sum2 = lst2.map(x => x * x).sum
    val zlst = lst1.zip(lst2)
    val product = zlst.map(x => x._1 * x._2).sum
    val numerator = product - (sum1 * sum2 / lst1.length)
    val dominator = pow((square_sum1 - pow(sum1, 2) / lst1.length) * (square_sum2 - pow(sum2, 2) / lst2.length), 0.5)
    val correlation = numerator / (dominator * 1.0)
    println(s"Correlation is: $correlation")
    correlation
  }

  def corr2ml(feat: RDD[(String, (Geometry, Map[String, Any]))], property1: String, property2: String): Double = {
    val time0: Long = System.currentTimeMillis()
    val aX: RDD[Double] = feat.map(t => t._2._2(property1).asInstanceOf[String].toDouble)
    val aY: RDD[Double] = feat.map(t => t._2._2(property2).asInstanceOf[String].toDouble)
    val correlation: Double = Statistics.corr(aX, aY, "pearson") //"spearman"
    val timeused: Long = System.currentTimeMillis() - time0
    println(s"Correlation is: $correlation")
    println(s"time used is: $timeused")
    correlation
  }
}
