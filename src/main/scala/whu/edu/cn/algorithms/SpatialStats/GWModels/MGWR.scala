package whu.edu.cn.algorithms.SpatialStats.GWModels

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Service
import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, qr, sum, trace}

import scala.collection.mutable
import scala.math._

class MGWR(inputRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))]) extends GWRbasic(inputRDD) {

  val maxIter:Int=10000
  var bwOptiArr:Array[Array[Double]]=_
  var bwOptiMap:Array[mutable.Map[Int, Double]]=_

  var bwArr1:Array[Double]=_
  var _approach:String=_

  var bw0:Double=0

  var mbetas:Array[Array[Double]]=_

  def fitAll(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true)={
    _kernel=kernel
    _adaptive=adaptive
    val bwselect = bandwidthSelection(kernel = kernel, approach = approach, adaptive = adaptive)
    bw0=bwselect
    val newWeight = if (_adaptive) {
      setWeight(round(bwselect), _kernel, _adaptive)
    } else {
      setWeight(bwselect, _kernel, _adaptive)
    }
    println(bw0)
    fitFunction(weight = newWeight)

  }

  def backfitting(iter:Int)={
    val re0=fitAll()
    val betas=re0._1
    val betasT: Array[Array[Double]] = betas.map(_.toArray).transpose
    val resid=re0._3
//    bwArr1(iter)=0
    val rss = resid.toArray.map(t => t * t).sum
    val criterion = 1e50
    for(i<-0 until _cols){
      val mXi=_dvecX(i)
      val fi=DenseVector(betasT(i)) *:* mXi
//      val fi=(betasT(i) zip mXi).map { case (x1, x2) => x1 * x2 }
      val mYi = resid + fi
      println(mYi)

      val bw1 = bandwidthSelection(_kernel, _approach, _adaptive)
      println(bw1)
      val newWeight = if (_adaptive) {
        setWeight(round(bw1), _kernel, _adaptive)
      } else {
        setWeight(bw1, _kernel, _adaptive)
      }
      val betai = fitFunction(weight = newWeight)._1
      val betaiT=betasTrans(betai)(i)
//      betas.zipWithIndex.foreach(t=>t._1(t._2)=betaiT(t._2))
//      val resid2 = _dvecY - getYHat(_dmatX, betas)
      println(betai)
      println(resid)
//      println(resid2)


    }

    def betasTrans(betas: Array[DenseVector[Double]])={
      betas.map(_.toArray).transpose.map(DenseVector(_))
    }


  }

  def regress()={
    val re0=fitAll()
//    val shat0=re0._4
//    val cmat0=re0._5
//    val idm=DenseMatrix.eye[Double](_cols)
//    val mSArray = Array.fill(_cols)(DenseMatrix.rand[Double](shat0.rows, shat0.cols))
    for(i<-0 to maxIter){
//      _nameX.map(t=>{
        backfitting(i)
//      })
    }

  }

}

object MGWR {

  def regress(sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyY: String, propertiesX: String,
              kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = false) = {
    val model = new MGWR(featureRDD)
    model.setY(propertyY)
    model.setX(propertiesX)
    val re0=model.fitAll(kernel = kernel, approach = approach, adaptive = adaptive)


//    Service.print(re._2, "Multiscale GWR", "String")
//    sc.makeRDD(re._1)
  }

}