//run describe
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.debug.GWmodelUtil.BasicStatistics.descriptive_statistics.describe
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil

import scala.collection.immutable.List
import scala.collection.mutable.Map


//run kde
//import geotrellis.raster.{ColorMap, ColorRamps, RasterExtent, Tile}
//import geotrellis.raster.mapalgebra.focal.Kernel
//import geotrellis.vector.{Extent, Feature, Point, PointFeature}
//import geotrellis.vector._
//import scala.util._
//import geotrellis.raster._
//import scala.util.Random

object testrun_lqy{
  def main(args: Array[String]):Unit={
    //读取数据
    val shppath:String="testdata\\LNHP100.shp"//自己改路径
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val shpfile : RDD[(String, (Geometry, Map[String, Any]))] = ShapeFileUtil.readShp(sc,shppath,ShapeFileUtil.DEF_ENCODE)//或者直接utf-8
    val list:List[Any]=Feature.get(shpfile,"PURCHASE")
    //数据类型转换
    val list_double:List[Double]=list.collect({ case (i: String) => (i.toDouble) })
    val list_rdd:RDD[Double]=sc.makeRDD(list_double)

    describe(list_rdd,list_double,10)
  }

//  //kde
//  def main(args: Array[String]):Unit= {
//    val extent=Extent(-109,37,-102,41)
//
//    def randomPointFeature(extent: Extent):PointFeature[Double]={
//      def randInRange(low:Double,high:Double):Double={
//        val x=Random.nextDouble
//        low*(1-x)+high*x
//      }
//      Feature(Point(randInRange(extent.xmin,extent.xmax),
//        randInRange(extent.ymin,extent.ymax)),
//        Random.nextInt%16+16)
//    }
//    val pts=(for(i <- 1 to 1000) yield randomPointFeature(extent)).toList
//    //println(pts)
//
//    val kernelWidth:Int=99
//    val kern:Kernel=Kernel.gaussian(kernelWidth,20,50)
//    val kde:Tile=pts.kernelDensity(kern,RasterExtent(extent,700,400))
//
//    val colorMap=ColorMap(
//      (0 to kde.findMinMax._2 by 4).toArray,
//      ColorRamps.HeatmapBlueToYellowToRedSpectrum
//    )
//    kde.renderPng(colorMap).write("test.png")
//  }
}
