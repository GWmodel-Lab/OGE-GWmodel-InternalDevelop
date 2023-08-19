package whu.edu.cn.debug.GWmodelUtil.BasicStatistics

import breeze.plot.{Figure, HistogramBins, hist}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil

import scala.collection.immutable.List
import scala.collection.mutable.Map

object descriptive_statistics{

  //主函数
  def main(args: Array[String]):Unit={
    //读取数据
    val shppath:String="C:\\Users\\Cheska\\Desktop\\研0暑假\\OGE\\测试数据\\LNHP100\\LNHP100.shp"//自己改路径
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val shpfile : RDD[(String, (Geometry, Map[String, Any]))] = ShapeFileUtil.readShp(sc,shppath,ShapeFileUtil.DEF_ENCODE)//或者直接utf-8
    val list:List[Any]=Feature.get(shpfile,"PURCHASE")
    //数据类型转换
    val list_double:List[Double]=list.collect({ case (i: String) => (i.toDouble) })
    //val list_rdd:RDD[Double]=sc.parallelize(list_double)
    val list_rdd:RDD[Double]=sc.makeRDD(list_double)

    describe(list_rdd,list_double,10)
  }

  //描述性统计
  //输入：RDD[Double],List[Double],m_bins:直方图的条数
  //输出：描述性统计结果，直方图
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
