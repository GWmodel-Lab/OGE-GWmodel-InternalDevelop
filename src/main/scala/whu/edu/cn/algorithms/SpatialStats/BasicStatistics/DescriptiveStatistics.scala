package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import breeze.plot.{Figure, HistogramBins, hist}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity.OGEClassType.Service

import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.Map

import whu.edu.cn.oge.Feature


object DescriptiveStatistics {

  /** Descriptive statistics for specific property of feature
   *
   * @param featureRDD shapefile
   * @return pic and count, sum, stdev .etc
   */
  def result(featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]): String = {
    val name = featureRDD.map(t => t._2._2.keySet.toList).collect().toList.head
    val n = name.length

    var str = f"\n**********descriptive statistics result**********\n"
    //println(str)
    for (m <- 0.to(n - 1)) {
      val list = featureRDD.map(t => t._2._2(name(m)).asInstanceOf[String])
      val b = list.first().toCharArray

      if (b(0) <= 57 && b(0) >= 48) {
        val list = featureRDD.map(t => t._2._2(name(m)).asInstanceOf[String].toDouble)
        val stats = list.stats()

        str += f"property : ${name(m)}\n"
        str += f"count : ${stats.count }\n"
        str += f"sum : ${stats.sum }\n"
        str += f"stdev : ${stats.stdev}\n"
        str += f"variance : ${stats.variance}\n"
        str += f"max : ${stats.max}\n"
        str += f"min : ${stats.min}\n\n"
        //print(str)

      }
      else{
        str += f"property : ${name(m)}\n"
        str += f"type: string\n\n"
        //print(str)

      }

    }
    print(str)
    str
  }
}

