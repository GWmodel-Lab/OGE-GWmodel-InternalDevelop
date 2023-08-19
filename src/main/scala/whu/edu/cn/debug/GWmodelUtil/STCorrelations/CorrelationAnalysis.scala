package whu.edu.cn.debug.GWmodelUtil.STCorrelations

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.Map

object CorrelationAnalysis {
  /**
   * 输入RDD和属性字符串数组，进行相关性分析，输出相关性矩阵Array[Array[Double]
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @return Array[Array[Double]] 结果cArr[i][j]保存了第i个属性和第j个属性之间的相关性数值
   */
  def corr(testshp: RDD[(String, (Geometry, Map[String, Any]))] , pArr: Array[String]): Unit = {
    var num = pArr.length
    var cArrSpearman = Array.ofDim[Double](num,num) //correlation
    var cArrPearson = Array.ofDim[Double](num,num) //correlation
    var i = 0;
    var j = 0;
    println("The correlation matrix:")
    for (i <- 0 to (num - 1)) {
      print(pArr(i) + " ")
    }
    println
    //循环输出相关性矩阵
    for (i <- 0 to (num - 1)) {
      for (j <- 0 to (num - 1)) {
        cArrSpearman(i)(j) = corr2ml(testshp,pArr(i),pArr(j),"spearman")
        cArrPearson(i)(j) = corr2ml(testshp,pArr(i),pArr(j),"pearson")
      }
    }
    println("Spearman correlation")
    for (i <- 0 to (num - 1)) {
      for (j <- 0 to (num - 1)) {
        print(cArrSpearman(i)(j).formatted("%.4f") + " ") //保留四位小数输出
      }
      println
    }
    println("Pearson correlation")
    for (i <- 0 to (num - 1)) {
      for (j <- 0 to (num - 1)) {
        print(cArrPearson(i)(j).formatted("%.4f") + " ") //保留四位小数输出
      }
      println
    }
  }

  /**
   * 对RDD的属性property1和property2进行求解得到之间的相关性，并且可以选用Spearman/Pearson
   *
   * @param feat : RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @param property1 : String的形式
   * @param property1 : String的形式
   * @param method : String的形式
   * @return Double 结果correlation为两属性之间的相关性
   */
  def corr2ml(feat: RDD[(String, (Geometry, Map[String, Any]))], property1: String, property2: String, method: String): Double = {
    val time0: Long = System.currentTimeMillis()
    val aX: RDD[Double] = feat.map(t => t._2._2(property1).asInstanceOf[String].toDouble)
    val aY: RDD[Double] = feat.map(t => t._2._2(property2).asInstanceOf[String].toDouble)
    val correlation: Double = Statistics.corr(aX, aY, method) //"spearman"/"pearson"
    correlation
  }
}