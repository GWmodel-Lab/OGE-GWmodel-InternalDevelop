package whu.edu.cn.debug.GWmodelUtil.BasicStatistics

import breeze.plot.{Figure, HistogramBins, hist}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.List


object descriptive_statistics{
  //描述性统计
  /**
    *
    * @param listrdd:RDD[Double],created by list[Double]
    * @param list:List[Double]
    * @param m_bins:number of histogram bins
    */
  def describe(listrdd:RDD[Double],list:List[Double],m_bins:HistogramBins):Unit={
    val stats=listrdd.stats()
    val sum=listrdd.sum()
    val variance=listrdd.variance()
    println(stats)
    println("sum",sum)
    println("variance",variance)

    val f=Figure()
    val p=f.subplot(0)
    p+=hist(list,m_bins)
    p.title="histogram"
    f.saveas("hist.png")

  }
}
