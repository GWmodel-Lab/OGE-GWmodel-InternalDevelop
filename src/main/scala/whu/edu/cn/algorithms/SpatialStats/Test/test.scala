package whu.edu.cn.algorithms.SpatialStats.Test

import breeze.linalg.{DenseMatrix, DenseVector, norm, normalize}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Point}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.{AverageNearestNeighbor, DescriptiveStatistics}
import whu.edu.cn.algorithms.SpatialStats.BasicStatistics.PrincipalComponentAnalysis.PCA
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.CorrelationAnalysis.corrMat
import whu.edu.cn.algorithms.SpatialStats.STCorrelations.TemporalAutoCorrelation._
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.LinearRegression
import whu.edu.cn.algorithms.SpatialStats.SpatialRegression.LogisticRegression
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

    GTWR.fit(sc,shpfile3,"PURCHASE","FLOORSZ,UNEMPLOY,PROF","TYPEDETCH", bandwidth=40,adaptive=true, lambda = 1.0)
    //    GWDA.calculate(sc,shpfile3,"TYPEDETCH","FLOORSZ,UNEMPLOY,PROF",kernel = "bisquare",method = "wlda")
    //    GWCorrelation.cal(sc, shpfile, "aging", "GDP,pop", 20, kernel = "bisquare", adaptive = true)
    //    GWAverage.cal(sc, shpfile, "aging", "GDP,pop", 50)
    //    LinearRegression.LinearReg(sc, shpfile, "aging", "PCGDP,GI,FD,education")
    //    LogisticRegression.LogisticRegression(sc, shpfile3,y="TYPEFLAT", x="FLOORSZ,PROF,UNEMPLOY")
    //    AverageNearestNeighbor.result(shpfile)
    //    DescriptiveStatistics.describe(shpfile)
    //    SpatialAutoCorrelation.globalMoranI(shpfile, "aging", plot = false, test = true)
    //    SpatialAutoCorrelation.localMoranI(shpfile, "aging")
    //    TemporalAutoCorrelation.ACF(shpfile, "aging", 20)
    //    CorrelationAnalysis.corrMat(shpfile, "aging,GDP,pop,GI,sci_tech,education,revenue", method = "spearman")
    //    GWRbasic.auto(sc, shpfile, "aging", "PCGDP,GI,FD,TS,CL,PCD,PIP,SIP,TIP,education", kernel = "bisquare")
    //    GWRbasic.fit(sc, shpfile, "aging", "PCGDP,GI,FD,education", 50, adaptive = true)
    //    GWRbasic.autoFit(sc, shpfile, "aging", "PCGDP,GI,FD,education",approach = "AICc", adaptive = true)
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

//    newGWR()

    val tused = (System.currentTimeMillis() - t1) / 1000.0
    println(s"time used is $tused s")
    sc.stop()
  }

  def newGWR()= {
//    val model=new GWRbasic(shpfile3)
//    model.setX("FLOORSZ")
//    model.setY("PURCHASE")
//    model.setWeight(bw = 98, kernel = "gaussian", adaptive = true)
//    val res=model.fitFunction()
//    val res_s=model.calDiagnostic(model.getX,model.getY,res._3,res._4)
//    println(res_s)
//    println("-------------")
//    val res2=GWRbasic.auto(sc, shpfile3, "PURCHASE", "FLOORSZ,UNEMPLOY,PROF",adaptive = true)
    val newRDD = shpfile3.map { case (id, (geometry, attributes)) =>
      val centroid = geometry.getCentroid()
      val newCoordinate = new Coordinate(centroid.getX + 500, centroid.getY + 500)
      val newPoint = geometry match {
        case point: Point => centroid.getFactory.createPoint(newCoordinate)
        case _ =>
          geometry
      }
      (id, (newPoint, attributes))
    }
    GWRbasic.predict(sc, shpfile3, newRDD, "PURCHASE", "FLOORSZ,UNEMPLOY,PROF",bandwidth = 50, kernel = "gaussian", adaptive = true)
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