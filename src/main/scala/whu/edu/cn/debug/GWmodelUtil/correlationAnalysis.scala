package whu.edu.cn.debug.GWmodelUtil

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil

import scala.collection.immutable.List
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.io.StdIn
import scala.math.{abs, max, min, pow, sqrt}
import scala.reflect.ClassTag
//import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.stat.Statistics
import breeze.numerics._
import breeze.linalg.{Vector, DenseVector, Matrix , DenseMatrix}

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._
import scala.util.control.Breaks._
object correlationAnalysis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val shpPath: String = "testdata\\LNHP.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
    val shpfile = ShapeFileUtil.readShp(sc,shpPath,ShapeFileUtil.DEF_ENCODE)//或者直接utf-8
    println(getGeometryType(shpfile))

    var pArr: Array[String] = Array[String]("PROF","FLOORSZ","UNEMPLOY","PURCHASE")//属性字符串数组
    var cArr = Array.ofDim[Double](100,100)//correlation
    var i = 0;
    var j = 0;
    var num = pArr.length
    //    println("Please input the property：")
    //    breakable{
    //      for (i <- 0 to 100) {
    //        pArr(i) = StdIn.readLine()
    //        if (pArr(i) == " ") {
    //          break;
    //        }
    //        num += 1
    //      }
    //    }
    println("The correlation matrix:")
    for (i <- 0 to (num - 1)){
      print(pArr(i)+" ")
    }
    println
    //循环输出相关性矩阵
    for (i <- 0 to (num - 1)){
      for (j <- 0 to (num - 1)){
        cArr(i)(j)=testcorr(shpfile, pArr(i), pArr(j),num)
      }
    }

    for (i <- 0 to (num - 1)) {
      for (j <- 0 to (num - 1)) {
        print(cArr(i)(j).formatted("%.4f")+" ")//保留四位小数输出
      }
      println
    }
  }

  def testcorr(testshp: RDD[(String, (Geometry, Map[String, Any]))], p1:String, p2:String, n:Int) : Double = {
    val list1:List[Any] = Feature.get(testshp,p1)
    val list2:List[Any] = Feature.getNumber(testshp,p2)
    val lst1:List[Double] = list1.collect({ case (i: String) => (i.toDouble) })
    val lst2:List[Double] = list2.asInstanceOf[List[Double]]
    val corr = corr2list(lst1,lst2)
    corr
  }

  def corr2list(lst1: List[Double], lst2: List[Double]): Double = {//返回correlation，给数组依次赋值
    val sum1 = lst1.sum
    val sum2 = lst2.sum
    val square_sum1 = lst1.map(x => x * x).sum
    val square_sum2 = lst2.map(x => x * x).sum
    val zlst = lst1.zip(lst2)
    val product = zlst.map(x => x._1 * x._2).sum
    val numerator = product - (sum1 * sum2 / lst1.length)
    val dominator = pow((square_sum1 - pow(sum1, 2) / lst1.length) * (square_sum2 - pow(sum2, 2) / lst2.length), 0.5)
    val correlation = numerator / (dominator * 1.0)
    correlation
  }
}