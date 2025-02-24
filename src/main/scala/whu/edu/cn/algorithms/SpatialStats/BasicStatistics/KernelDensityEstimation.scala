package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import breeze.linalg.{DenseVector, _}
import breeze.numerics._
import breeze.signal._
import breeze.stats._
import breeze.interpolation._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.GWModels.Algorithm
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.LogisticRegression.setX
import whu.edu.cn.oge.Service

import scala.math.BigDecimal
import scala.collection.mutable
import scala.math.{Pi, exp, pow, sqrt}


object KernelDensityEstimation extends Algorithm {
  private var _data: RDD[mutable.Map[String, Any]] = _
  private var _dvecX: DenseVector[Double] = _
  private var _nameX: String = _

  override def setX(property: String, split: String = ","): Unit = {
    _nameX = property
    _dvecX = DenseVector(_data.map(t => t(property).asInstanceOf[String].toDouble).collect())
  }

  /**
   *
   * @param sc SparkContext
   * @param data RDD
   * @param x 输入X
   * @param bw 带宽，默认为-1，若bw<=0，则根据输入数据优选带宽bw.nrd0
   * @param n 估计密度时的等距点数，默认512
   * @param cut 决定分析的区间，区间上下界为输入数据的最值加/减cut*bw，cut默认为3
   * @param kernel 核函数类型，默认为gaussian，此外还包括rectangular, triangular,epanechnikov,biweight,cosine及optcosine
   * @return
   */
  def fit(sc: SparkContext, data: RDD[(String, (Geometry, mutable.Map[String, Any]))],x: String,
          bw: Double = -1,n: Int = 512, cut: Int = 3, kernel: String = "gaussian"):Unit = {
    _data = data.map(t => t._2._2)
    setX(x)
    val X = _dvecX

    val bandwidth = if(bw <=0) bw_nrd0(sc,X) else bw
    val nPoints = n
    val minData = X.toArray.min
    val maxData = X.toArray.max

    val gridMin = minData-cut*bandwidth
    val gridMax = maxData+cut*bandwidth
    val grid = breeze.linalg.linspace(gridMin, gridMax, nPoints)    // x of result
    val x_rdd = DenseVectorToRDD(sc,grid)

    // kde basic
    val density = kde(X.toArray,grid.toArray,bandwidth,kernel)
    val y_rdd = DenseVectorToRDD(sc,DenseVector(density))

    val x_min = grid.toArray.min
    val x_max = grid.toArray.max
    val x_mean = mean(grid)
    val x_1qu = computePercentile(x_rdd,25)
    val x_median = computePercentile(x_rdd,50)
    val x_3qu = computePercentile(x_rdd,75)

    val y_min = density.min
    val y_max = density.max
    val y_mean = mean(density)
    val y_1qu = computePercentile(y_rdd, 25)
    val y_median = computePercentile(y_rdd, 50)
    val y_3qu = computePercentile(y_rdd, 75)

    // output string
    val resMat = DenseMatrix.zeros[String](7,3)
    resMat(0,0) = ""
    resMat(0,1) = "x"
    resMat(0,2) = "y"

    resMat(1,0) = "Min."
    resMat(2,0) = "1st Qu."
    resMat(3,0) = "Median"
    resMat(4,0) = "Mean"
    resMat(5,0) = "3rd Qu."
    resMat(6,0) = "Max."

    resMat(1, 1) = x_min.toString
    resMat(2, 1) = x_1qu.toString
    resMat(3, 1) = x_median.toString
    resMat(4, 1) = x_mean.toString
    resMat(5, 1) = x_3qu.toString
    resMat(6, 1) = x_max.toString

    resMat(1, 2) = y_min.toString
    resMat(2, 2) = y_1qu.toString
    resMat(3, 2) = y_median.toString
    resMat(4, 2) = y_mean.toString
    resMat(5, 2) = y_3qu.toString
    resMat(6, 2) = y_max.toString

    val str = "\n**************Result of Kernel Density Estimation**************\n"+
      f"Number of Samples: ${X.length}; Bandwidth: ${bandwidth.formatted("%.4f")}\n"+
      f"Kernel: $kernel\n"+
      f"$resMat\n"+
              "***************************************************************\n"
    println(str)

    }

  def kernelSelection(name: String): Double => Double = {
    name match {
      case "gaussian"     => (u:Double) => (1.0 / sqrt(2 * Pi)) * exp(-0.5 * u * u)
      case "rectangular"  => (u:Double) => if(scala.math.abs(u)>1) 0 else 0.5
      case "triangular"   => (u:Double) => if(scala.math.abs(u)>1) 0 else 1-scala.math.abs(u)
      case "epanechnikov" => (u:Double) => if(scala.math.abs(u)>1) 0 else 0.75*(1-u*u)
      case "biweight"     => (u:Double) => if(scala.math.abs(u)>1) 0 else 15.0/16.0*(1 - u*u)*(1 - u*u)
      case "cosine"       => (u:Double) => if(scala.math.abs(u)>1) 0 else 0.5*(1+cos(Pi * u))
      case "optcosine"    => (u:Double) => if(scala.math.abs(u)>1) 0 else Pi/4*cos(Pi/2*u)
      case _ => throw new IllegalArgumentException("Unsupported Kernel")
    }
  }

  def kde(data: Array[Double], grid: Array[Double], bandwidth: Double, kernel: String): Array[Double] = {
    val n = data.length
    val K = kernelSelection(kernel)
    grid.map { point =>
      data.map { xi =>
        val u = (point - xi) / bandwidth
        K(u)
      }.sum / (n * bandwidth)
    }
  }

  def bw_nrd0(sc: SparkContext,X:DenseVector[Double]):Double={
    if(X.length < 2) throw new IllegalArgumentException("require at least 2 samples")
    val stddev = breeze.stats.stddev(X)
    var lo = math.min(stddev,computePercentile(DenseVectorToRDD(sc,X),75)/1.34)
    if(lo == 0) {
      lo = stddev
      if(lo == 0){
        lo = math.abs(X(1))
        if(lo == 0) {
          lo = 1
        }
      }
    }
    0.9 * lo * math.pow(X.length,-0.2)
  }

  // modifying
  /**
   * compute percentile from an unsorted Spark RDD
   *
   * @param data : input data set of Long integers
   * @param tile : percentile to compute, [0,100]
   * @return value of input data at the specified percentile
   */
  def computePercentile(data: RDD[Double], tile: Double): Double = {
    // NIST method; data to be sorted in ascending order
    val percentile = if(tile <0) 0 else if (tile >100) 100 else tile
    val sortedData = data.sortBy(x => x)
    val count = sortedData.count()

    if (count == 0) {
      throw new IllegalArgumentException("Data RDD is empty")
    }

    val index = percentile/100.0 * (count -1)
    val lowerIndex = math.floor(index).toLong
    val upperIndex = math.ceil(index).toLong

    val indices = sortedData.zipWithIndex().map(_.swap)
    if (lowerIndex == upperIndex) {
      indices.lookup(lowerIndex).head
    } else {
      val lowerValue = indices.lookup(lowerIndex).head
      val upperValue = indices.lookup(upperIndex).head
      lowerValue + (upperValue - lowerValue) * (index - lowerIndex)
    }
  }

  def DenseVectorToRDD(sc: SparkContext, data:DenseVector[Double]):RDD[Double]={
    sc.parallelize(data.toArray)
  }

  def formatToSignificantFigures(value: Double, significantFigures: Int): String = {
    val precision =  BigDecimal(value).precision
    val scale = significantFigures - precision + 1
    println(scale)
    BigDecimal(value).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toString
  }

}
