package whu.edu.cn.debug.GWmodelUtil.STCorrelations

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature

import scala.collection.mutable.Map
import scala.math.pow
object GWMcorrelation {
  /**
   * 输入RDD，进行相关性分析，输出相关性矩阵Array[Array[Double]]
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @return Array[Array[Double]] 结果cArr[i][j]保存了第i个属性和第j个属性之间的相关性数值
   */
  def corr(testshp: RDD[(String, (Geometry, Map[String, Any]))]): Array[Array[Double]] = {
    var pArr: Array[String] = Array[String]("PROF", "FLOORSZ", "UNEMPLOY", "PURCHASE") //属性字符串数组
    var num = pArr.length
    var cArr = Array.ofDim[Double](num,num) //correlation
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
   * 输入RDD，求解得到属性p1和p2之间的相关性
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @param p1 String的形式
   * @param p2 String的形式
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
   * 对lst1和lst2两组数据进行求解得到之间的相关性
   *
   * @param lst1: List[Double]的形式
   * @param lst2: List[Double]的形式
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