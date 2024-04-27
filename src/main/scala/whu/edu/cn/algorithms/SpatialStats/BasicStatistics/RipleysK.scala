package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance.{arrayDist, getDist}

import scala.math.{max, min, sqrt}
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.util.Random

object RipleysK {

  /** Ripley's K
   *
   * @param featureRDD  输入
   * @param nTimes      间隔数
   * @param nTests      测试数
   * @return
   */
  def ripley(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], nTimes: Int = 10, nTests: Int = 9): String = {
    val coords = featureRDD.map(t => t._2._1.getCoordinate)
    val nCounts = featureRDD.count().toInt
    val extents = coords.map(t => {
      (t.x, t.y, t.x, t.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val sum = coords.map(t => {
      (t.x, t.y)
    }).reduce((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })
    //    val center=(sum._1/nCounts,sum._2/nCounts)//结果还是求所有点的，不需要求质心
    //    val distCenter=coords.map(t=>{
    //      sqrt((t.x-center._1)*(t.x-center._1)+(t.y-center._2)*(t.y-center._2))
    //    })
    val dMat = getDist(featureRDD) //使用距离矩阵。因为i≠j，所以最后要减去一个nCounts
    val area = (extents._3 - extents._1) * (extents._4 - extents._2)
    //    val maxDs=sqrt((extents._3-extents._1)*(extents._3-extents._1)+(extents._4-extents._2)*(extents._4-extents._2))*0.25
    val maxDs = max((extents._3 - extents._1), (extents._4 - extents._2)) * 0.25
    val adds = maxDs / nTimes
    val rek = calculate(dMat, area, nCounts, adds, nTimes)

    //random test
    // TODO: max min correct, output.
//    val maxk = rek.map(t => {
//      (t._1, t._2, t._2)
//    }).zipWithIndex
//    for (i <- 1 until nTests) {
//      val randp = randomPoints(extents._1, extents._2, extents._3, extents._4, nCounts)
//      val dist = arrayDist(randp, randp)
//      val r = calculate(dist, area, nCounts, adds, nTimes)
//
//      maxk.map(t => {
//        (t._1._1, min(t._1._2, r(t._2)._2), max(t._1._3, r(t._2)._2))
//      })
//      println(r.toList)
//      println(maxk.toVector)
//    }
    val reStr="dist,   k-value\n"+rek.mkString("\n")
    println(reStr)
    reStr
  }

  def randomPoints(xmin: Double, ymin: Double, xmax: Double, ymax: Double, np: Int): Array[(Double, Double)] = {
    Array.fill(np)(Random.nextDouble(), Random.nextDouble()).map(t => (t._1 * (xmax - xmin) + xmin, t._2 * (ymax - ymin) + ymin))
  }

  def calculate(dMat:Array[Array[Double]],area:Double,nCounts:Int, adds:Double, nTimes:Int): Array[(Double, Double)] = {
    val rek=new Array[(Double,Double)](nTimes)
    for (i <- 1 to nTimes) {
      val iDs = adds * i
      val iK = dMat.map(t => {
        t.map(t2 => {
          if (t2 < iDs) {
            1.0
          }
          else {
            0.0
          }
        }).sum
      }).sum
      //      println(iK.toList)
      val k = sqrt(area * (iK - nCounts) / math.Pi / nCounts / (nCounts - 1))
      rek(i-1)=(iDs,k)
//      println(iDs, k)
    }
    rek
  }


}
