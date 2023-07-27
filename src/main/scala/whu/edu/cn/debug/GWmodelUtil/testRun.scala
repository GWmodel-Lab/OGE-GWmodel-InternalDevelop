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
import breeze.linalg.{Vector, DenseVector, Matrix , DenseMatrix}
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._
import whu.edu.cn.debug.GWmodelUtil.sp_autocorrelation._
import whu.edu.cn.debug.GWmodelUtil.other_util._

object testRun {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    val shpPath: String = "testdata\\MississippiHR.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
//    val shpPath: String = "testdata\\LNHP.shp"
    val shpfile = readShp(sc,shpPath,DEF_ENCODE)//或者直接utf-8
    val geom=println(getGeometryType(shpfile))

    val globali = globalMoranI(shpfile, "HR60")
    println(s"global Moran's I is: ${globali._1}")

//    val locali=localMoranI(shpfile,"HR60")
//    println("-----------local moran's I--------------")
//    locali._1.foreach(println)

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

    val tem=attributeSelectHead(csvdata,"temperature")
//    tem.foreach(println)
    val db_tem=tem.map(t=>t.toDouble)
    println(db_tem.sum)
    val tem_acf = timeseiresacf(db_tem,5)
    println(s"temperature acf is $tem_acf")

//    val aqi = attributeSelectNum(csvdata, 2)
////    aqi.foreach(println)
//    val db_aqi = aqi.map(t => t.toDouble)
//    println(db_aqi.sum)

  }

  def readtimeExample(sc: SparkContext)={
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
