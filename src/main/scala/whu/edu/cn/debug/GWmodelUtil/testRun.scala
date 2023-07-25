package whu.edu.cn.debug.GWmodelUtil

import geotrellis.vector.MultiPolygon
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry, LineString}
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
    val geom=println(getGeometryType(shpfile))
    println("-------------")
    val locali=localMoranI(shpfile,"HR60")
    println("-----------local moran's I--------------")
//    locali._1.foreach(println)

    val shp = shpfile.map(t => t._2._2).collect()
    for (i <- 0 until locali._1.length) {
      shp(i) += ("locali" -> locali._1(i))
    }
//    println("-------shp-----------")
//    shp.foreach(println)

    val outdata = shpfile.collect().zipWithIndex
    outdata.map(t => {
      t._1._2._2 += ("locali"->locali._1(t._2))
    })
    val result=sc.makeRDD(outdata.map(t=>t._1))
    println("-------------result------------------")
    result.collect().foreach(println)

    createShp("testdata\\MississippiMorani.shp","utf-8",classOf[MultiPolygon],result.map(t => {
      t._2._2 + (DEF_GEOM_KEY -> t._2._1)
    }).collect().map(_.asJava).toList.asJava)
    println("成功落地shp")

//    var data=shpfile.collect()
//    data.map(shpt=>{
////      shpt._2._2 += ("locali",0.0)
//      shpt._2._2=shp.foreach(t=>t)
//    })
//    println("-------------re------------------")
//    data.map(t=>t._2._2).foreach(println)


//    val globali = globalMoranI(shpfile, "HR60")
//    println(s"global Moran's I is: $globali")

    //    val geom=getGeometry(shpfile)
    //    val nb=getNeighborBool(geom)
    //    val idx=boolNeighborIndex(nb).collect()
    //    printArrArr(idx)

//    val time1: Long = System.currentTimeMillis()
//    val rdddist=getRDDDistRDD(sc,shpfile)
////    printArrArr(rdddist.collect())
//    val time2: Long = System.currentTimeMillis()
//    val rddweight=getSpatialweight(rdddist,50,"gaussian",true)
//    rddweight.collect().foreach(println)
//    val time3: Long = System.currentTimeMillis()
//    println(time3-time2,time2-time1)

  }

  def makenewdata(data: Array[Map[String, Any]], geom: Array[Geometry])={
        for (i <- 0 until geom.length) {
          data.map(t=>{
            (geom(i),t)
          })

    }
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
