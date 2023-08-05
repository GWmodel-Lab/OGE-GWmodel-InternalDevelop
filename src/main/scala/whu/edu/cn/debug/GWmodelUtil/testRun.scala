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
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.mllib.stat.Statistics
import breeze.numerics._
import breeze.linalg.{linspace, Vector, DenseVector, Matrix , DenseMatrix}
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import breeze.plot._

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._
import whu.edu.cn.debug.GWmodelUtil.sp_autocorrelation._
import whu.edu.cn.debug.GWmodelUtil.other_util._

import whu.edu.cn.debug.GWmodelUtil.SARmodels

object testRun {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    val shpPath: String = "testdata\\MississippiHR.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
//    val shpPath: String = "testdata\\LNHP100.shp"
    val shpfile = readShp(sc,shpPath,DEF_ENCODE)//或者直接utf-8
    println(getGeometryType(shpfile))

//    val globali = globalMoranI(shpfile, "HR60",plot=true,test=true)
//    println(s"global Moran's I is: ${globali._1}")
//
//    val locali=localMoranI(shpfile,"HR60")
//    println("-----------local moran's I--------------")
//    locali._1.foreach(println)
//    println("-----------p-value--------------")
//    locali._5.foreach(println)

//    val result1=writeRDD(sc,shpfile,locali._1,"moran_i")
//    val result2=writeRDD(sc,result1,locali._2,"expect")
//    val outputpath="testdata\\MississippiMoranI.shp"
//    writeshpfile(result2,outputpath)

//    testcorr(shpfile)

    val csvpath="D:\\Java\\testdata\\test_aqi.csv"
    val csvdata=readcsv(sc,csvpath)
//    printArrArr(csvdata.collect())

//    //test date calculator
//    val timep=attributeSelectHead(csvdata,"time_point")
//    val timepattern = "yyyy/MM/dd"
//    val date=timep.map(t=>{
//      val date = new SimpleDateFormat(timepattern).parse(t)
//      date
//    })
//    date.foreach(println)
//    println((date(300).getTime - date(0).getTime)/1000/60/60/24)

//    val tem=attributeSelectHead(csvdata,"temperature")
////    tem.foreach(println)
//    val db_tem=tem.map(t=>t.toDouble)
////    println(db_tem.sum)
//    val tem_acf = timeSeries_acf(db_tem,10)
//    tem_acf.foreach(println)
//
//    val aqi = attributeSelectNum(csvdata, 2).map(t=>t.toDouble)
////    aqi.foreach(println)
//    val per=attributeSelectHead(csvdata, "precipitation").map(t=>t.toDouble)
//
//    val x=Array(DenseVector(db_tem),DenseVector(per))
//    val re=global_regression.linearRegression(x,DenseVector(aqi))
//    println(re._1)
//    println(re._2)
//    println(re._3)

//    test class of sarmodels
    val x1=shpfile.map(t => t._2._2("PO60").asInstanceOf[String].toDouble).collect()
    val x2=shpfile.map(t => t._2._2("UE60").asInstanceOf[String].toDouble).collect()
    val y =shpfile.map(t => t._2._2("HR60").asInstanceOf[String].toDouble).collect()
    val x=Array(DenseVector(x1),DenseVector(x2))
//    x.foreach(println)
    var mdl=new SARlagmodel
    mdl.init(shpfile)
    mdl.setX(x)
    mdl.setY(y)
    mdl.setweight()
//    val a=mdl.rho4optimize(-1.742396)
//    println(a)
//    val inte=mdl.getinterval()
//    println(inte)
//    val rho=mdl.goldenSelection(inte._1,inte._2)
    mdl.fit()
    val betas=DenseVector(0.001,0.05)
    val rho=0.5
    mdl.nelderMead(rho, betas)
  }

  def readtimeExample(sc: SparkContext): Unit = {
    val csvpath = "D:\\Java\\testdata\\test_aqi.csv"
    val csvdata = readcsv(sc, csvpath)

    val timep = attributeSelectHead(csvdata, "time_point")
    val timepattern = "yyyy/MM/dd"
    val date = timep.map(t => {
      val date = new SimpleDateFormat(timepattern).parse(t)
      date
    })
    date.foreach(println)
    println((date(300).getTime - date(0).getTime) / 1000 / 60 / 60 / 24)
  }

}
