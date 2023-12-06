package whu.edu.cn.algorithms.SpatialStats.Test

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.{AverageNearestNeighbor, DescriptiveStatistics}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.PrincipalComponentAnalysis.PCA
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.CorrelationAnalysis.corrMat
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.TemporalAutoCorrelation._
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.LinearRegression.linearRegression
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.{SpatialDurbinModel, SpatialErrorModel, SpatialLagModel}
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils._
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWRbasic
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWAverage
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWCorrelation
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.{CorrelationAnalysis, SpatialAutoCorrelation, TemporalAutoCorrelation}
import whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity.Geodetector
import whu.edu.cn.algorithms.SpatialStats.STSampling.SandwichSampling
import whu.edu.cn.oge.Feature._
import whu.edu.cn.util.ShapeFileUtil._

object test {
  //global variables
  val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
      .set("spark.testing.memory", "512000000")
  val sc = new SparkContext(conf)
  val encode="utf-8"

  val shpPath: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\cn_aging.shp"
  val shpfile = readShp(sc, shpPath, encode)

  val shpPath2: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\MississippiHR.shp"
  val shpfile2 = readShp(sc, shpPath2, encode)

  val shpPath3: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\LNHP100.shp"
  val shpfile3 = readShp(sc, shpPath3, encode)

  val csvpath = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\test_aqi.csv"
  val csvdata = readcsv(sc, csvpath)
  //写成无参数的函数形式来进行测试，方便区分，以后可以改成 catch...if... 形式

  def main(args: Array[String]): Unit = {

    val t1 = System.currentTimeMillis()
    //    acf_test()
    //    linear_test()
    //    pca_test()
    //    geodetector_test()
    //    GWCorrelation.cal(sc, shpfile, "aging", "GDP,pop", bw=20, kernel = "bisquare", adaptive = true)
    //    GWAverage.cal(sc, shpfile, "aging", "GDP,pop", 50)

    //    AverageNearestNeighbor.result(shpfile)
    //    DescriptiveStatistics.describe(shpfile)
    //    SpatialAutoCorrelation.globalMoranI(shpfile, "aging", plot = false, test = true)
    //    SpatialAutoCorrelation.localMoranI(shpfile, "aging")
    //    TemporalAutoCorrelation.ACF(shpfile, "aging", 20)
    //    CorrelationAnalysis.corrMat(shpfile, "aging,GDP,pop,GI,sci_tech,education,revenue", method = "spearman")
    //    GWRbasic.auto(sc, shpfile, "aging", "PCGDP,GI,FD,TS,CL,PCD,PIP,SIP,TIP,education", kernel = "bisquare")
    //    GWRbasic.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education", 10, adaptive = true)
    //    GWRbasic.autoFit(sc, shpfile, "aging", "PCGDP,GI,FD,education",approach = "CV", adaptive = true)
    //    SpatialLagModel.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education")
    //    SpatialErrorModel.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education")
    //    SpatialDurbinModel.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education")

    //    val r=readcsv2(sc,csvpath)
    //    linearRegression(r,"aqi","temperature,precipitation")

    Geodetector.factorDetector(shpfile3, "PURCHASE", "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW")
    Geodetector.interactionDetector(shpfile3, "PURCHASE", "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW")
    Geodetector.ecologicalDetector(shpfile3, "PURCHASE", "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW")
    Geodetector.riskDetector(shpfile3, "PURCHASE", "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW")
    SandwichSampling.sampling(sc, shpfile3,"PURCHASE", "FLOORSZ", "TYPEDETCH")

    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
    sc.stop()
  }

  def gwrbasic_test(): Unit = {
    val t1 = System.currentTimeMillis()
    val mdl = new GWRbasic
    mdl.init(shpfile)
    mdl.setX("FLOORSZ,PROF,UNEMPLOY,CENTHEAT,BLD90S,TYPEDETCH")
    mdl.setY("PURCHASE")
    //    val re=mdl.fit(bw = 10000,kernel="bisquare",adaptive = false)
    //    val bw=mdl.bandwidthSelection(adaptive = false)
    //    mdl.fit(bw = bw,kernel="gaussian",adaptive = false)
    mdl.variableSelect()
    //    mdl.auto(kernel="gaussian",approach = "CV", adaptive = false)
    //    val re_rdd=sc.makeRDD(re)
    //    writeshpfile(re_rdd,"D:\\Java\\testdata\\re_gwr.shp")
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

  def pca_test():Unit= {
    PCA(shpfile)
  }

  def acf_test(): Unit = {
    val t1 = System.currentTimeMillis()
    //test date calculator
    /*
    val timep = attributeSelectHead(csvdata, "time_point")
    val timepattern = "yyyy/MM/dd"
    val date = timep.map(t => {
      val date = new SimpleDateFormat(timepattern).parse(t)
      date
    })
    date.foreach(println)
    println((date(300).getTime - date(0).getTime) / 1000 / 60 / 60 / 24)
     */
    val tem = attributeSelectHead(csvdata, "temperature")
    val db_tem = tem.map(t => t.toDouble)
    //    println(db_tem.sum)
    val tem_acf = timeSeriesACF(db_tem, 30)
    //    tem_acf.foreach(println)
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

  def linear_test(): Unit = {
    val t1 = System.currentTimeMillis()
    val aqi = attributeSelectNum(csvdata, 2).map(t => t.toDouble)
    val per = attributeSelectHead(csvdata, "precipitation").map(t => t.toDouble)
    val tem = attributeSelectHead(csvdata, "temperature").map(t => t.toDouble)
    val x = Array(DenseVector(tem), DenseVector(per))
    //    val re = linearRegression(x, DenseVector(aqi))
    //    println(re._1)
    //    println(re._2)
    //    println(re._3)
    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
  }

  def geodetector_test():Unit ={
    var t1 = System.currentTimeMillis()
    val y_title = "PURCHASE"
    val x_titles = "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW"
    val FD = Geodetector.factorDetector(shpfile, y_title, x_titles)
    val ID = Geodetector.interactionDetector(shpfile, y_title, x_titles)
    val ED = Geodetector.ecologicalDetector(shpfile, y_title, x_titles)
    val RD = Geodetector.riskDetector(shpfile, y_title, x_titles)
    var tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is: $tused s")
  }

}