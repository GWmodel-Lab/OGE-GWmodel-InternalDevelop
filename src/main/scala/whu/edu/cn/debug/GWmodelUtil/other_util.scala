package whu.edu.cn.debug.GWmodelUtil

import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature

import java.io.{StringReader, StringWriter}
import au.com.bytecode.opencsv._
import breeze.linalg.{DenseVector, linspace}
import breeze.plot._

import scala.collection.mutable.Map
import scala.math.pow
import scala.reflect.ClassTag

object other_util {

  def readcsv(implicit sc: SparkContext, csvPath: String): RDD[Array[(String, Int)]]={
    val data = sc.textFile(csvPath)
    val csvdata = data.map(line => {
      val reader = new CSVReader(new StringReader((line)))
      reader.readNext()
    })
    csvdata.map(t=>t.zipWithIndex)
  }

  def timeSeries_acf(timeArr: Array[Double], timelag: Int=20): Array[Double] = {
    val acfarr = DenseVector.zeros[Double](timelag+1).toArray
    if (timelag > 0) {
      val f = Figure()
      val p = f.subplot(0)
      for (i <- 0 until timelag+1){
        acfarr(i)=getacf(timeArr,i)
        val x=DenseVector.ones[Double](5) :*= i.toDouble
        val y=linspace(0,acfarr(i),5)
        p += plot(x,y,colorcode="[0,0,255]")
      }
      p.xlim = (-0.1,timelag+0.1)
      p.xlabel = "lag"
      p.ylabel = "ACF"
    } else {
      throw new IllegalArgumentException("Illegal Argument of time lag")
    }
    acfarr
  }

  def getacf(timeArr: Array[Double], timelag: Int): Double={
    val lagArr=timeArr.drop(timelag)
    val tarridx=timeArr.take(lagArr.length).zipWithIndex
    val avg = timeArr.sum / timeArr.length
    val tarr_avg=timeArr.map(t=>t*avg).sum
    val tarr_avg2=tarridx.map(t=>t._1*avg).sum
    val lag_avg=lagArr.map(t=>t*avg).sum
//    var tarr_lag:Double =0.0
//    for(i<-0 until lagArr.length){
//      tarr_lag += timeArr(i)*lagArr(i)
//    }
    val tarr_lag=tarridx.map(t=>t._1*lagArr(t._2)).sum
    val acf=(tarr_lag-tarr_avg2-lag_avg+pow(avg,2)*lagArr.length)/(timeArr.map(t=>t*t).sum-2*tarr_avg+pow(avg,2)*timeArr.length)
    acf
  }

  def attributeSelectHead(csvRDD: RDD[Array[(String, Int)]], property: String): Array[String] = {
    val head = csvRDD.collect()(0)
    val csvArr = csvRDD.collect().drop(1)
    var resultArr = new Array[String](csvArr.length)
    var idx: Int = -1
    head.map(t => {
      if (t._1 == property) idx = t._2
    })
    //    println(idx)
    if (idx == -1) {
      throw new IllegalArgumentException("property name didn't exist!!")
    } else {
      val df = csvArr.map(t => t.filter(i => i._2 == idx).map(t => t._1))
      resultArr = df.flatMap(t => t)
    }
    //    resultArr.foreach(println)
    resultArr
  }

  def attributeSelectNum(csvRDD: RDD[Array[(String, Int)]], number: Int): Array[String] = {
    val head = csvRDD.collect()(0)
    val csvArr = csvRDD.collect().drop(1)
    var resultArr = new Array[String](csvArr.length)
    val idx: Int = number - 1 //从1开始算
    //    println(idx)
    if (idx >= head.length || idx < 0) {
      throw new IllegalArgumentException("property number didn't exist!!")
    } else {
      val df = csvArr.map(t => t.filter(i => i._2 == idx).map(t => t._1))
      resultArr = df.flatMap(t => t)
    }
    //    resultArr.foreach(println)
    resultArr
  }

  /**
   * 输入要添加的属性数据和RDD，输出RDD
   *
   * @param sc           SparkContext
   * @param shpRDD       源RDD
   * @param writeArray   要写入的属性数据，Array形式
   * @param propertyName 属性名，String类型，需要少于10个字符
   * @return RDD
   */
  def writeRDD(sc: SparkContext, shpRDD: RDD[(String, (Geometry, Map[String, Any]))], writeArray: Array[Double], propertyName: String): RDD[(String, (Geometry, Map[String, Any]))] = {
    if (propertyName.length >= 10) {
      throw new IllegalArgumentException("the length of property name must not longer than 10!!")
    }
    val shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.map(t => {
      t._1._2._2 += (propertyName -> writeArray(t._2))
    })
    sc.makeRDD(shpRDDidx.map(t => t._1))
  }

  def printArrArr[T: ClassTag](arrarr: Array[Array[T]]) = {
    val arrvec = arrarr.map(t => t.toVector)
    arrvec.foreach(println)
  }

  def testcorr(testshp: RDD[(String, (Geometry, Map[String, Any]))]): Unit = {
    val time0: Long = System.currentTimeMillis()
    val list1: List[Any] = Feature.get(testshp, "PURCHASE") //FLOORSZ,PROF
    val list2: List[Any] = Feature.getNumber(testshp, "FLOORSZ")
    //    println(list1)
    //    println(list2)
    val lst1: List[Double] = list1.collect({ case (i: String) => (i.toDouble) })
    //    //val lst2:List[Double]=list2.collect({ case (i: String) => (i.toDouble) })
    val lst2: List[Double] = list2.asInstanceOf[List[Double]]
    val corr1 = corr2list(lst1, lst2)
    val listtimeused = System.currentTimeMillis() - time0
    println(s"time used is: $listtimeused")

    val coor2 = corr2ml(testshp, "PURCHASE", "FLOORSZ")

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