//run describe
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.debug.GWmodelUtil.BasicStatistics.descriptive_statistics.describe
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil

import scala.collection.immutable.List
import scala.collection.mutable.Map

object testrun_lqy {
  def main(args: Array[String]): Unit = {
    //读取数据
    val shppath: String = "testdata\\LNHP100.shp"
    //自己改路径
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)
    val shpfile: RDD[(String, (Geometry, Map[String, Any]))] = ShapeFileUtil.readShp(sc, shppath, ShapeFileUtil.DEF_ENCODE)
    //或者直接utf-8
    val list: List[Any] = Feature.get(shpfile, "PURCHASE")
    //数据类型转换
    val list_double: List[Double] = list.collect({ case (i: String) => (i.toDouble) })
    val list_rdd: RDD[Double] = sc.makeRDD(list_double)

    describe(list_rdd, list_double, 10)
  }
}