package whu.edu.cn.debug.GWmodelUtil

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Geometry}
import whu.edu.cn.oge.Feature
import whu.edu.cn.util.ShapeFileUtil
import scala.collection.immutable.List
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{abs, max, min, pow, sqrt}

//import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.stat.Statistics
import breeze.numerics._
import breeze.linalg.{Vector, DenseVector, Matrix , DenseMatrix}

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

object testRun {
  def main(args: Array[String]): Unit = {
    //    val time1: Long = System.currentTimeMillis()
    //    println(time1)]
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
    val sc = new SparkContext(conf)

    val shpPath: String = "testdata\\MississippiHR.shp" //我直接把testdata放到了工程目录下面，需要测试的时候直接使用即可
    val shpfile = ShapeFileUtil.readShp(sc,shpPath,ShapeFileUtil.DEF_ENCODE)//或者直接utf-8

    val geom=shpfile.map(t=>t._2._1)
    val nb=getNeighborBool(geom)
    val arrnb=nb.map(t=>t.toVector).collect()
    arrnb.foreach(println)
    val idx=boolNeighborIndex(nb)
    val arridx=idx.map(t=>t.toVector).collect()
    arridx.foreach(println)
    val weinb=boolNeighborWeight(nb)
    val arrweinb=weinb.map(t=>t.toVector).collect()
    arrweinb.foreach(println)

    //    val testshp=shpfile.map(t=>{
    //      val geom=t._2._1
    //      val prop=t._2._2("Avg_AQI")
    //      (geom,prop)
    //    })
//    val rdddist=getRDDDistRDD(sc,shpfile,shpfile)
//    val rddweight:RDD[DenseVector[Double]]=spatialweightRDD(rdddist,10,"bisquare",true)
//
//    rddweight.collect().foreach(println)
//    val arrweight=rddweight.flatMap(t=>DenseVector2Array(t)).collect()


//    val apoint=getCoorXY(shpfile)
//    val aX=apoint.take(1)
//    val arr0=arrayDist(aX,apoint)
//    //    arr0.foreach(println)
//    val dv: DenseVector[Double] = new DenseVector(arr0)
////    arr0.sorted.take(20).foreach(println)
//    val weight = spatialweightSingle(dv,10,"bisquare",true)
//    weight.foreach(println)

//    println("gaussianKernelFunction")
//    val weight1 = GaussianKernelFunction(dv,bw)
//    weight1.foreach(println)
//    println("exponentialKernelFunction")
//    val weight2 = ExponentialKernelFunction(dv, bw)
//    weight2.foreach(println)
//    println("BisquareKernelFunction")
//    val weight3 = bisquareKernelFunction(dv, bw)
//    weight3.foreach(println)
//    println("TricubeKernelFunction")
//    val weight4 = tricubeKernelFunction(dv, bw)
//    weight4.foreach(println)
//    println("BoxcarKernelFunction")
//    val weight5 = boxcarKernelFunction(dv, bw)
//    weight5.foreach(println)

//    val arr4=apoint.take(4)
//    val arr3=apoint.take(3)
//    println("arr4")
//    arr4.foreach(println)
//    println("arr3")
//    arr3.foreach(println)
//    val arrd=arrayDist(arr4,arr3)
//    println("dis")
//    arrd.foreach(println)
//    val dmat1= new DenseMatrix(arr3.length, arr4.length, arrd)
//    println(dmat1.transpose)
//    val dmat2 = getArrDistDmat(arr4,arr3)
//    println(dmat2)

  }

  def testcorr(testshp: RDD[(String, (Geometry, Map[String, Any]))]) : Unit={
    val time0: Long = System.currentTimeMillis()
    val list1:List[Any] = Feature.get(testshp,"PURCHASE")//FLOORSZ,PROF
    val list2:List[Any] = Feature.getNumber(testshp,"FLOORSZ")
//    println(list1)
//    println(list2)
    val lst1:List[Double] = list1.collect({ case (i: String) => (i.toDouble) })
//    //val lst2:List[Double]=list2.collect({ case (i: String) => (i.toDouble) })
    val lst2:List[Double] = list2.asInstanceOf[List[Double]]
    val corr1 = corr2list(lst1,lst2)
    val listtimeused= System.currentTimeMillis()-time0
    println(s"time used is: $listtimeused")

    val coor2 = corr2ml(testshp,"PURCHASE","FLOORSZ")

  }

  def corr2list(lst1: List[Double], lst2: List[Double]): Double = {
    val sum1 = lst1.sum
    val sum2 = lst2.sum
    val square_sum1 = lst1.map(x => x * x).sum
    val square_sum2 = lst2.map(x => x * x).sum
    val zlst = lst1.zip(lst2)
    val product = zlst.map(x => x._1 * x._2).sum
    val numerator = product - (sum1 * sum2 / lst1.length)
    val dominator = pow((square_sum1 - pow(sum1, 2) / lst1.length) * (square_sum2 - pow(sum2, 2) / lst2.length), 0.5)
    val correlation = numerator / (dominator * 1.0)
    println(s"Correlation is: $correlation")
    correlation
  }

  def corr2ml(feat: RDD[(String, (Geometry, Map[String, Any]))], property1: String, property2: String): Double = {
    val time0: Long = System.currentTimeMillis()
    val aX: RDD[Double] = feat.map(t => t._2._2(property1).asInstanceOf[String].toDouble)
    val aY: RDD[Double] = feat.map(t => t._2._2(property2).asInstanceOf[String].toDouble)
    val correlation: Double = Statistics.corr(aX, aY, "pearson") //"spearman"
    val timeused: Long = System.currentTimeMillis() - time0
    println(s"Correlation is: $correlation")
    println(s"time used is: $timeused")
    correlation
  }
}
