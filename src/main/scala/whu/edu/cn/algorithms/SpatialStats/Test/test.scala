package whu.edu.cn.algorithms.SpatialStats.Test

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.{AverageNearestNeighbor, DescriptiveStatistics}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.PrincipalComponentAnalysis.PCA
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.CorrelationAnalysis.corrMat
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.TemporalAutoCorrelation._
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.LinearRegression
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.{SpatialDurbinModel, SpatialErrorModel, SpatialLagModel}
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils._
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWRbasic
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWDA
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWAverage
import whu.edu.cn.algorithms.SpatialStats.GWModels.GWCorrelation
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.{CorrelationAnalysis, SpatialAutoCorrelation, TemporalAutoCorrelation}
import whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity.Geodetector
import whu.edu.cn.algorithms.SpatialStats.STSampling.SandwichSampling
import whu.edu.cn.oge.Feature._
import whu.edu.cn.util.ShapeFileUtil._
import breeze.linalg.{norm, normalize}
import breeze.numerics._

object test {
  //global variables
  val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("query")
//      .set("spark.testing.memory", "512000000")
  val sc = new SparkContext(conf)
  val encode="utf-8"

  val shpPath: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\cn_aging.shp"
  val shpfile = readShp(sc, shpPath, encode)

  val shpPath2: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\MississippiHR.shp"
  val shpfile2 = readShp(sc, shpPath2, encode)

//  val shpPath3: String = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\LNHP100.shp"
//  val shpPath3="D:\\Java\\testdata\\USelect2004part.shp"
  val shpPath3="D:\\Java\\testdata\\LNHP100.shp"
  val shpfile3 = readShp(sc, shpPath3, encode)

  val csvpath = "src\\main\\scala\\whu\\edu\\cn\\algorithms\\SpatialStats\\Test\\testdata\\test_aqi.csv"
  val csvdata = readcsv(sc, csvpath)
  //写成无参数的函数形式来进行测试，方便区分，以后可以改成 catch...if... 形式

  def main(args: Array[String]): Unit = {

    val t1 = System.currentTimeMillis()
    //    acf_test()
    //    linear_test()
    //    pca_test()

    val m=new GWDA
    m.init(shpfile3)
//    m.setX("unemploy,pctcoled,PEROVER65,pcturban,WHITE")
//    m.setY("winner")
    m.setX("FLOORSZ,UNEMPLOY,PROF")
    m.setY("CLASS")
    m.setweight(50,"bisquare",true)
    m.getYLevels()
    m.getXGroups()
    m.wlda()
//    m.wqdaCr(50)

//    println("========")
//    val a=DenseVector(1.37942984198612E11, 9.004055022233693E7, 1926260.9417264587, 9.004055022233693E7, 68150.40821476714, 2038.5591051096094, 1926260.9417264587, 2038.5591051096094, 823.7534907059759)
//    val b=(0.01 * a)
//    println(b)
//    val c=DenseMatrix.create(3,3,b.toArray)
//    println(norm(b),log(norm(b)),normalize(b))

    //    GWCorrelation.cal(sc, shpfile, "aging", "GDP,pop", bw=20, kernel = "bisquare", adaptive = true)
    //    GWAverage.cal(sc, shpfile, "aging", "GDP,pop", 50)
    //    LinearRegression.LinearReg(shpfile,"aging", "GDP,pop")
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

    //    println(Geodetector.factorDetector(shpfile3, "PURCHASE", "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW"))
    //    println(Geodetector.interactionDetector(shpfile3, "PURCHASE", "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW"))
    //    println(Geodetector.ecologicalDetector(shpfile3, "PURCHASE", "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW"))
    //    println(Geodetector.riskDetector(shpfile3, "PURCHASE", "FLOORSZ,TYPEDETCH,TYPETRRD,TYPEBNGLW,BLDPOSTW"))
    //    println(Geodetector.factorDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,SIP,TIP,PIP,pop,city,employee"))
    //    println(Geodetector.interactionDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,SIP,TIP,PIP,pop,city,employee"))
    //    println(Geodetector.ecologicalDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,SIP,TIP,PIP,pop,city,employee"))
//        println(Geodetector.riskDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,SIP,TIP,PIP,pop,city,employee"))
    //    println(Geodetector.riskDetector(shpfile, "GDP", "province"))
    //    println(Geodetector.interactionDetector(shpfile2, "HR60", "PO60,DV60,STATE_NAME"))
    //    val rddSample=SandwichSampling.sampling(sc, shpfile3,"PURCHASE", "FLOORSZ", "TYPEDETCH")
    //    rddSample.foreach(println)

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

}