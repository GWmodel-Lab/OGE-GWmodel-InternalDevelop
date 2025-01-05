package whu.edu.cn.algorithms.SpatialStats.Test

import breeze.linalg.{DenseMatrix, DenseVector, norm, normalize}
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
import whu.edu.cn.algorithms.SpatialStats.GWModels.GTWR
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.{CorrelationAnalysis, SpatialAutoCorrelation, TemporalAutoCorrelation}
import whu.edu.cn.algorithms.SpatialStats.STSampling.Sampling.{randomSampling, regularSampling, stratifiedSampling}
import whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity.Geodetector
import whu.edu.cn.algorithms.SpatialStats.STSampling.SandwichSampling
import whu.edu.cn.oge.Feature._
import whu.edu.cn.util.ShapeFileUtil._
import breeze.numerics._
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight.getSpatialweight

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


//    GTWR.fit(sc,shpfile3,"y","x1,x2,x3","t", bandwidth=100,adaptive=true, lambda = 0.5)
    //    GWDA.calculate(sc,shpfile3,"TYPEDETCH","FLOORSZ,UNEMPLOY,PROF",kernel = "bisquare",method = "wlda")
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
    //    println(Geodetector.riskDetector(shpfile, "aging", "PCGDP,GI,FD,education,GDP,province,SIP,TIP,PIP,pop,city,employee"))
    //    println(Geodetector.riskDetector(shpfile, "GDP", "province"))
    //    println(Geodetector.interactionDetector(shpfile2, "HR60", "PO60,DV60,STATE_NAME"))
    //    val rddSample=SandwichSampling.sampling(sc, shpfile3,"PURCHASE", "FLOORSZ", "TYPEDETCH")
    //    rddSample.foreach(println)

    test()

    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
    sc.stop()
  }

  def test()= {
    val rd = getDistRDD(shpfile3)
    //    rd.foreach(println)
    val weight = getSpatialweight(rd, bw = 20, kernel = "gaussian", adaptive = true)
    //    weight.collect().foreach(println)
    GWRbasic.fit(sc, shpfile3, "PURCHASE", "FLOORSZ,UNEMPLOY,PROF", 20, adaptive = true)
    val model=new GWRbasic
    model.init(shpfile3)
    model.setX("FLOORSZ,UNEMPLOY,PROF")
    model.setY("PURCHASE")
    val res=model.fitRDDFunction(weight=weight)
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