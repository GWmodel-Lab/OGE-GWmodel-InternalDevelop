package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import scala.math.{min,max,sqrt}
import scala.collection.mutable.Map

object RipleysK {


  //todo: 逐个点计算，双循环
  def ripley(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], nTimes:Int=10)={
    val coords=featureRDD.map(t=>t._2._1.getCoordinate)
    val nCounts= featureRDD.count().toInt
    val extents = coords.map(t => {
      (t.x, t.y, t.x, t.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val sum=coords.map(t=>{
      (t.x,t.y)
    }).reduce((x,y) =>{
      (x._1+y._1,x._2+y._2)
    })
    val center=(sum._1/nCounts,sum._2/nCounts)//结果还是求所有点的，不需要求质心
    val distCenter=coords.map(t=>{
      sqrt((t.x-center._1)*(t.x-center._1)+(t.y-center._2)*(t.y-center._2))
    })
    println(extents)
    println(center)
    val area=(extents._3-extents._1)*(extents._4-extents._2)
//    val maxDs=sqrt((extents._3-extents._1)*(extents._3-extents._1)+(extents._4-extents._2)*(extents._4-extents._2))*0.25
    val maxDs=max((extents._3-extents._1),(extents._4-extents._2))*0.25
    val adds=maxDs/nTimes
    println(area,nCounts,adds)
    for(i<-1 to nTimes){
      val iDs=adds*i
      val iK=distCenter.map(t=>{
        if(t<iDs){1.0}else{0.0}
      }).collect()
//      println(iK.toList)
      val k=sqrt(area*iK.sum/math.Pi/nCounts/(nCounts-1))
      println(iDs,k)
    }

  }

}
