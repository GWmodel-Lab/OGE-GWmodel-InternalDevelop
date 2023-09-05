package whu.edu.cn.debug.SpatialStats.STCorrelations

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import breeze.linalg.DenseMatrix
import org.locationtech.jts.geom.{Coordinate, Geometry}
import whu.edu.cn.oge.Feature
import whu.edu.cn.oge.Feature._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{abs, max, min, pow, sqrt}
import scala.collection.mutable.Map

object CorrelationAnalysis {

  def corrMat(inputshp: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyArr: Array[String], method: String = "pearson"): DenseMatrix[Double] = {
    val n = propertyArr.length
    var cor = new Array[Array[Double]](n)
    if (method == "pearson") {
      val arrList = propertyArr.map(t => getNumber(inputshp, t))
      //    arrList.foreach(println)
      //如果是皮尔逊相关系数才可以用这个
      for (i <- 0 until n) {
        cor = arrList.map(t => {
          arrList.map(t2 => corr2list(t2, t))
        })
      }
    } else {
      val arrRDD = propertyArr.map(p => {
        inputshp.map(t => t._2._2(p).asInstanceOf[String].toDouble)
      })
      //    arrRDD.map(t=>t.collect().foreach(println))
      for (i <- 0 until n) {
        cor = arrRDD.map(t => {
          arrRDD.map(t2 => Statistics.corr(t2, t, method = method))
        })
      }
    }
    //    cor.map(t=>t.foreach(println))
    val corrMat = DenseMatrix.create(rows = n, cols = n, data = cor.flatten)
    println(s"$method correlation result:")
    propertyArr.map(t => printf("%-20s\t", t))
    print("\n")
    println(corrMat)
    corrMat
  }

  /**
   * 输入RDD，进行相关性分析，输出相关性矩阵Array[Array[Double]]
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @return Array[Array[Double]] 结果cArr[i][j]保存了第i个属性和第j个属性之间的相关性数值
   */
  def corr(testshp: RDD[(String, (Geometry, Map[String, Any]))]): Array[Array[Double]] = {
    var pArr: Array[String] = Array[String]("PROF", "FLOORSZ", "UNEMPLOY", "PURCHASE") //属性字符串数组
    var num = pArr.length
    var cArr = Array.ofDim[Double](num, num) //correlation
    var i = 0;
    var j = 0;
    /*
    var cArrSpearman = Array.ofDim[Double](num, num) //correlation
    var cArrPearson = Array.ofDim[Double](num, num) //correlation
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
        cArrSpearman(i)(j) = corr2ml(testshp, pArr(i), pArr(j), "spearman")
        cArrPearson(i)(j) = testcorr(testshp, pArr(i), pArr(j))
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
    */
    println("The correlation matrix:")
    for (i <- 0 to (num - 1)) {
      print(pArr(i) + " ")
    }
    println
    //循环输出相关性矩阵
    for (i <- 0 to (num - 1)) {
      for (j <- 0 to (num - 1)) {
        cArr(i)(j) = testcorr(testshp, pArr(i), pArr(j))
      }
    }

    for (i <- 0 to (num - 1)) {
      for (j <- 0 to (num - 1)) {
        print(cArr(i)(j).formatted("%.4f") + " ") //保留四位小数输出
      }
      println
    }
    cArr
  }

  /**
   * 对RDD的属性property1和property2进行求解得到之间的相关性，并且可以选用Spearman/Pearson
   *
   * @param feat : RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @param property1 : String的形式
   * @param property2 : String的形式
   * @param method : String的形式
   * @return Double 结果correlation为两属性之间的相关性
   */
  def corr2ml(feat: RDD[(String, (Geometry, Map[String, Any]))], property1: String, property2: String, method: String): Double = {
    val aX: RDD[Double] = feat.map(t => t._2._2(property1).asInstanceOf[String].toDouble)
    val aY: RDD[Double] = feat.map(t => t._2._2(property2).asInstanceOf[String].toDouble)
    Statistics.corr(aX, aY, method) //"spearman"
  }

  /**
   * 输入RDD，求解得到属性p1和p2之间的pearson相关性
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @param p1      String的形式
   * @param p2      String的形式
   * @return Double 结果corr为两个属性间的相关性
   */
  def testcorr(testshp: RDD[(String, (Geometry, Map[String, Any]))], p1: String, p2: String): Double = {
    val list1: List[Any] = Feature.get(testshp, p1)
    val list2: List[Any] = Feature.getNumber(testshp, p2)
    val lst1: List[Double] = list1.collect({ case (i: String) => (i.toDouble) })
    val lst2: List[Double] = list2.asInstanceOf[List[Double]]
    val corr = corr2list(lst1, lst2)
    corr
  }

  /**
   * 对lst1和lst2两组数据进行求解得到之间的pearson相关性
   *
   * @param lst1 : List[Double]的形式
   * @param lst2 : List[Double]的形式
   * @return Double 结果correlation为两组数据之间的相关性
   */
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
    correlation
  }
}