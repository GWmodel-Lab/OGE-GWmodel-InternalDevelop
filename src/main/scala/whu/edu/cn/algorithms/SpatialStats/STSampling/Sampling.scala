package whu.edu.cn.algorithms.SpatialStats.STSampling

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import scala.math.{min,max}
import scala.util.Random

object Sampling {

    def randomSampling(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], n: Int = 10)= {
      val coords = featureRDD.map(t => t._2._1.getCoordinate)
      val nCounts = featureRDD.count().toInt
      val extents = coords.map(t => {
        (t.x, t.y, t.x, t.y)
      }).reduce((coor1, coor2) => {
        (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
      })

    }


  def randomPoints(xmin: Double, ymin: Double, xmax: Double, ymax: Double, np: Int): Array[(Double, Double)] = {
    Array.fill(np)(Random.nextDouble(), Random.nextDouble()).map(t => (t._1 * (xmax - xmin) + xmin, t._2 * (ymax - ymin) + ymin))
  }



}
