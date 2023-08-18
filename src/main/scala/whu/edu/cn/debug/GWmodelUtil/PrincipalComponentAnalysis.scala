package whu.edu.cn.debug.GWmodelUtil

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import whu.edu.cn.oge.Feature
import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.util.ShapeFileUtil
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._


import breeze.linalg.{DenseMatrix, _}
import scala.collection.immutable

object PrincipalComponentAnalysis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val shpPath: String = "testdata\\LNHP.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
    val shpfile = ShapeFileUtil.readShp(sc, shpPath, ShapeFileUtil.DEF_ENCODE) //或者直接utf-8
    println(getGeometryType(shpfile))
    PCA(shpfile)
  }

  /**
   * 输入RDD，对数据进行主成分分析
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @return DenseMatrix[Double] 结果为降维处理后的数据
   */
  def PCA(testshp: RDD[(String, (Geometry, Map[String, Any]))]): DenseMatrix[Double] = {
    //原始的数据
    val pArr: Array[String] = Array[String]("PROF", "FLOORSZ", "UNEMPLOY", "PURCHASE") //属性字符串数组
    var lst = shpToList(testshp, pArr(0))
    val col = pArr.length
    val row = lst.length
    var i = 0;
    var j = 0;
    val matrix = new DenseMatrix[Double](row, col)
    for (i <- 0 to (col - 1)) {
      lst = shpToList(testshp, pArr(i))
      for (j <- 0 to (row - 1)) {
        matrix(j, i) = lst(j)
      }
    }
    //选择top2的特征
    val s = generatePCA(matrix, 2)
    s
  }

  /**
   * 从RDD中得到对应属性p的数据
   *
   * @param testshp RDD[(String, (Geometry, Map[String, Any]))]的形式
   * @param p String的形式
   * @return List[Double]
   */
  def shpToList(testshp: RDD[(String, (Geometry, Map[String, Any]))], p: String): List[Double] = {
    val list: List[Any] = Feature.get(testshp, p)
    val lst: List[Double] = list.collect({ case (i: String) => (i.toDouble) })
    lst
  }

  /**
   *
   *
   * @param matrix DenseMatrix[Double]的形式
   * @param indexes Arary[Int]的形式
   * @return DenseMatrix[Double]
   */
  def selectCols(matrix: DenseMatrix[Double], indexes: Array[Int]): DenseMatrix[Double] = {
    val ab = new ArrayBuffer[Double]
    for (index <- indexes) {
      val colVector = matrix(::, index).toArray
      ab ++= colVector
    }
    new DenseMatrix[Double](matrix.rows, indexes.length, ab.toArray)
  }

  /**
   * 选择topK的特征值，对矩阵A作主成分分析
   *
   * @param A DenseMatrix[Double]的形式
   * @param topK Int的形式
   * @return DenseMatrix[Double]
   */
  def generatePCA(A: DenseMatrix[Double], topK: Int): DenseMatrix[Double] = {

    val B = A.copy
    val avg = sum(B, Axis._0).inner.map(_ / B.rows)

    //中心化处理
    for (i <- 0 to B.rows - 1; j <- 0 to B.cols - 1) {
      val o = BigDecimal(B.apply(i, j))
      val jj = BigDecimal(avg.apply(j))
      B.update(i, j, (o - jj).toDouble)
    }
    val ratio = 1 / (B.rows.toDouble - 1)

    //求协方差矩阵
    val C = (B.t * B).map(_ * ratio)
    val featuresInfo = eigSym(C)
    val eigenvalues = featuresInfo.eigenvalues
//    println("特征值  " + eigenvalues)
    val eigenvectors: DenseMatrix[Double] = featuresInfo.eigenvectors
//    println("特征向量 " + eigenvectors)
    //降维处理
    val tuples: immutable.Seq[(Int, Double)] = for (i <- 0 to eigenvalues.toArray.length - 1) yield ((i, eigenvalues.toArray.apply(i)))
    val indexs: immutable.Seq[Int] = tuples.sortWith(((t1: (Int, Double), t2: (Int, Double)) => t1._2.compareTo(t2._2) > 0)).take(2).map(_._1)
    val D: DenseMatrix[Double] = selectCols(eigenvectors, indexs.toArray)

    println("原始数据")
    println(A)
    println("降维处理后")
    println(A*D)
    A * D
  }
}

