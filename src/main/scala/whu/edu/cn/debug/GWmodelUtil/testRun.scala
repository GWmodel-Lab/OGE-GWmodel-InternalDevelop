package whu.edu.cn.debug.GWmodelUtil

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.stat.Statistics

import scala.collection.mutable.{Map}
import scala.math.{abs, max, min, pow, sqrt}

object testRun {
  def main(args: Array[String]): Unit = {
    //    val time1: Long = System.currentTimeMillis()
    //    println(time1)
    val shppath: String = "D:\\Java\\testdata\\LNHP100.shp"
    val testshp=loadshp(shppath)
    mytest(testshp)
  }
  def loadshp (shpPath: String):RDD[(String, (Geometry, Map[String, Any]))]={
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val shpload = ShapeFileUtil.readShp(sc,shpPath,ShapeFileUtil.DEF_ENCODE)//或者直接utf-8
//    println(shpload.partitions.length)//划分了多少
    shpload
  }

  def mytest(testshp: RDD[(String, (Geometry, Map[String, Any]))]) : Unit={
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

  //这是用ml这个库写的

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
