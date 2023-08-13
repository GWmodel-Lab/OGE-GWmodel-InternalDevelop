package whu.edu.cn.debug.GWmodelUtil

import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.debug.GWMcorrelation
import whu.edu.cn.util.ShapeFileUtil
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._
object test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val shpPath: String = "testdata\\LNHP.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
    val shpfile = ShapeFileUtil.readShp(sc, shpPath, ShapeFileUtil.DEF_ENCODE) //或者直接utf-8
    println(getGeometryType(shpfile))
    GWMcorrelation.corr(shpfile)
  }
}