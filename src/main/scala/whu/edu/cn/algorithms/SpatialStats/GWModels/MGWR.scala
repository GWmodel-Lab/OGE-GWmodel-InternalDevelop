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
    println("Initializing...")
    _kernel=kernel
    _adaptive=adaptive
    val bwselect = bandwidthSelection(kernel = kernel, approach = approach, adaptive = adaptive)
    bw0=bwselect
    val newWeight = if (_adaptive) {
      setWeight(round(bwselect), _kernel, _adaptive)
    } else {
      setWeight(bwselect, _kernel, _adaptive)
    }
    println(f"Initial bandwidth: $bw0")
    fitFunction(weight = newWeight)

  }

  def backfitting(sc: SparkContext,iter:Int)={
    // _rows samples and _cols variables, dp.n = _rows and var.n = _cols
    val varN = _cols
    val dpN = _rows
    val re0=fitAll()
    val betas=re0._1
    var resid=re0._3
    var shat =re0._4 // n*n
    var S_array = new Array[DenseMatrix[Double]](dpN)//
    for(i<- 0 until dpN){
      S_array(i) = DenseMatrix.zeros[Double](varN,dpN)//p*n*n 4*100*100
    }
    var C    =(re0._5).map(_.t)
    val idm = DenseMatrix.eye[Double](varN)
//    // C，S_array,shat的维度完全是乱的，R代码复刻不了
//    println(f"dimension C: ${C(0).rows}, ${C(0).cols}, ${C.length}")
//    println(f"dimension S_array: ${S_array(0).rows}, ${S_array(0).cols}, ${S_array.length}")
//    println(f"dimension Shat: ${shat.rows}, ${shat.cols}")
//    for(i <- 0 until varN; j <- 0 until dpN){
//      val Cj = C.map(t => t(j,::).inner) // maybe need transpose
//      val matCj = DenseMatrix(Cj:_*).t
//      val idm_i = idm(i,::)
//      println(s"size of Cj: ${matCj.rows}, ${matCj.cols}")
//      println(s"size of idm_i: ${idm_i.inner.size}")
//      for(k <- 0 until dpN){
//        println(f"val S = _dmatX($j,$i) * (idm_$i * matC$j)")
//        val S = _dmatX(j,i) * (idm_i * matCj)
//        println(f"S_array($k)($i,$j) = S(0,$k)")
////        println(s"size of S: ${S.rows}, ${S.cols}")
//        S_array(k)(i,j) = S(0,k)
//      }
//
//    }


    val Y = _dvecY
    val betasT: Array[Array[Double]] = betas.map(_.toArray).transpose
    var rss0 = resid.toArray.map(t => t*t).sum
    val criterion = 1e7

    val matFi = DenseMatrix.zeros[Double](_rows,_cols)
    for(i<-0 until _cols){
      matFi(::,i) := DenseVector(betasT(i)) *:* _dvecX(i)
    }
    val matFi_old = DenseMatrix.zeros[Double](_rows,_cols)
    for (r <- 0 until _rows; c <- 0 until _cols) {
      matFi_old(r, c) = matFi(r, c)
    }

    val vecBw = DenseVector.zeros[Double](_cols)
    val matBetas = DenseMatrix.zeros[Double](_rows, _cols)

    // calculation
    var iter_present = 1
    var is_converged = false
    while(!is_converged && iter_present <= iter){
      println(f"---------------------------iteration $iter_present---------------------------")
      for (i <- 0 until _cols) {
        val varname = if(i==0)"Intercept" else _nameX(i-1)
        println(f"Select a bandwidth for variable ${i+1}/${_cols}: $varname")

        val mXi = _dvecX(i)
        val fi = matFi(::, i)
        val mYi = resid + fi

        // local regression between mYi and mXi
        // create a new RDD containing mXi and mYi for GWR
        val shpRDDidx = _shpRDD.collect().zipWithIndex
        shpRDDidx.foreach(t => t._1._2._2.clear())
        shpRDDidx.map(t => {
          t._1._2._2 += ("localX" -> mXi(t._2).toString)
          t._1._2._2 += ("localY" -> mYi(t._2).toString)
        })
        val localRDD = sc.makeRDD(shpRDDidx.map(t => t._1))
        val localGWR = new GWRbasic(localRDD)
        localGWR.setY("localY")
        localGWR.setX("localX")
        val localBw =  localGWR.getBandwidth(_kernel,_approach,_adaptive)

        val localWeight = if (_adaptive) {
          setWeight(round(localBw), _kernel, _adaptive)
        } else {
          setWeight(localBw, _kernel, _adaptive)
        }
        val localGWRRes = localGWR.fitFunction(weight = localWeight)
        val localBetas = localGWRRes._1.map(t =>t(1))
        val localYHat = localGWRRes._2
        val localResid = localGWRRes._3
        shat = localGWRRes._4
        val localCi = localGWRRes._5

//        val localAICc = localGWR.getAICc(localBw)
//        val localBetas = localGWR.fit(bw = localBw)._1.map(_._2._2.get("localX").get.asInstanceOf[Double])

        println(f"Newly selected bandwidth for $varname : $localBw")
//        println(f"localGWR aicc: ${localAICc}")
//        print(f"localBetas(first 5): ")
//        for (j <- 0 until 5) {
//          print(f"${localBetas(j)} ")
//        }
//        println(f"size of shat: ${shat.rows}, ${shat.cols}")

        //update fi
        matFi(::, i) := DenseVector(localBetas) *:* mXi
        vecBw(i) = localBw
        matBetas(::, i) := DenseVector(localBetas)
        resid = Y - DenseVector((0 until _rows).map(t => matFi(t, ::).inner.sum).toArray)

        //      val bw1 = bandwidthSelection(_kernel, _approach, _adaptive)// 这里写的好像是个”常量“，不进行迭代
        //      val newWeight = if (_adaptive) {
        //        setWeight(round(bw1), _kernel, _adaptive)
        //      } else {
        //        setWeight(bw1, _kernel, _adaptive)
        //      }
        //      val betai = fitFunction(weight = newWeight)._1
        //      val betaiT=betasTrans(betai)(i)
        //      betas.zipWithIndex.foreach(t=>t._1(t._2)=betaiT(t._2))
        //      val resid2 = _dvecY - getYHat(_dmatX, betas)

        //      println(mYi(0 until 10))
        //      println(fi)
        //      println(bw1)
        //      println(betai.toList(0))
        //      println(resid(0 until 10))
        //      println(resid2)
        println("--------------------------------------------")

      }
      val resid1 = Y - DenseVector((0 until _rows).map(t => matFi(t, ::).inner.sum).toArray)
      val rss1 = resid1.map(t =>t*t).sum
//      val cvr = abs(rss1 - rss0)
//      val dcvr = sqrt(abs(rss1-rss0)/rss1)
//      println(f"CVR: $cvr")
//      println(f"dCVR: $dcvr")
      is_converged = converge(matFi,matFi_old)
      for(r <- 0 until _rows;c <- 0 until _cols){
        matFi_old(r,c) = matFi(r,c)
      }
      iter_present = iter_present+1
      rss0 = rss1
    }
    println("Backfitting complete")
//    println(f"final bw: $vecBw")
//    println(f"final betas:\n$matBetas")
    val res = (matFi,vecBw,matBetas)
    res

//    def betasTrans(betas: Array[DenseVector[Double]])={
//      betas.map(_.toArray).transpose.map(DenseVector(_))
//    }

  }

  def regress(sc: SparkContext,iter: Int = 100)={
//    val re0=fitAll()
    //    val shat0=re0._4
    //    val cmat0=re0._5
    //    val idm=DenseMatrix.eye[Double](_cols)
    //    val mSArray = Array.fill(_cols)(DenseMatrix.rand[Double](shat0.rows, shat0.cols))
//    for(i<-0 to maxIter){
//      //      _nameX.map(t=>{
//      backfitting(sc,i)
//      //      })
//    }
    val bf = backfitting(sc, iter)
    val matFi = bf._1
    val vecBw = bf._2
    val matBetas = bf._3

    // get yhat, residuals
    val yhat = (0 until _rows).map(t=>{
      matFi(t,::).inner.sum
    }).toArray
    val residuals = _dvecY - DenseVector(yhat)
    val rss = residuals.map(t=>t*t).sum
    val n = _rows
    val p = _cols - 1
    val shat = sqrt(rss/(n-p-1))
//    val diag = calDiagnostic(_dmatX,_dvecY,residuals,shat)
//    println(f"yhat: ${yhat.toList}")
//    println(f"residuals: $residuals")
//    println(f"bandwidth: $vecBw")

  }

  protected def converge(Fi: DenseMatrix[Double], FiOld: DenseMatrix[Double], threshold: Double = 1e-5): Boolean = {
    val n = _rows
    val numerator = (Fi -:- FiOld).map(t => t * t).sum / n
    val denominator = (0 until _rows).map(t => {
      pow(Fi(t, ::).inner.sum, 2)
    }).sum
    val SOCf = sqrt(numerator / denominator)
    println(f"SOC-f: $SOCf")
    if (SOCf <= threshold) {
      true
    } else {
      false
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