package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg
import breeze.linalg.{DenseMatrix, DenseVector, inv, linspace, norm, sum}
import breeze.numerics.{NaN, log, sqrt}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class GWDA extends GWRbase {

  var _inputY:Array[Any]=_
  var _distinctLevels: Array[_ <: (Any, Int)]= _
  var _levelArr: Array[Int] = _
  var _dataLevels: Array[(String,Int,Int)] = _ //Array是 (键，键对应的int值（从0开始计数），对应的索引位置)
  var _dataGroups: Map[String, Array[(String, Int, Int)]] = _
  var _groups : Int = 0
  var _groupX:ArrayBuffer[Array[DenseVector[Double]]] = ArrayBuffer.empty[Array[DenseVector[Double]]]
  var spweight_groups: ArrayBuffer[Array[DenseVector[Double]]] = ArrayBuffer.empty[Array[DenseVector[Double]]]
  var SigmaGw: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
  var sigma_wlda: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
  var sigma_wqda: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
  var localPrior: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
  var localMean: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]

  var groupPf: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]

  override def setY(property: String): Unit = {
    _nameY = property
    _inputY = shpRDD.map(t => t._2._2(property)).collect()
  }

  def wqdaCr(bw:Double, kernel: String = "gaussian", adaptive: Boolean = true)={
    setweight(bw, kernel, adaptive)
//    val spweightIdx=spweight_dvec.zipWithIndex
//    spweightIdx.foreach(t=> {
//      spweight_dvec.foreach(w=>w(t._2)=0)
//    })
    for(i<-0 to _xrows){
      spweight_dvec.foreach(t=>t(i)=0)
    }
    spweight_dvec.foreach(t=>println(t))
  }

  def getLevels(data: Array[_ <: Any] = _inputY): Array[Int]={
    val data_idx=data.zipWithIndex
    val distin_idx=data.distinct.zipWithIndex
    val dlevel=data_idx.map(t=>{
      distin_idx.find(_._1 == data(t._2))
    })
//    println(dlevel.toVector)
    val levels=dlevel.map({
      getSomeInt(_)
    })
//    println(levels.toVector)
    _distinctLevels=distin_idx
    _levelArr=levels
    _dataLevels=dlevel.zipWithIndex.map(t=>((getSomeStr(t._1),getSomeInt(t._1),t._2)))
    _dataGroups=_dataLevels.groupBy(_._1)
    _groups=_dataGroups.size
    levels
  }

  def getSomeInt(x: Option[(Any, Int)]): Int = x match {
    case Some(s) => s._2
    case None => -1
  }

  def getSomeStr(x: Option[(Any, Int)]): String = x match {
    case Some(s) => s._1.toString
    case None => null
  }

//  def getSomeAll(x: Option[(Any, Int)]): (String,Int) = x match {
//    case Some(s) => (s._1.toString,s._2)
//    case None => (null,-1)
//  }

  def valSplit(x:Array[DenseVector[Double]] =_X)= {
    val nlevel = _distinctLevels.length
    val arrbuf = ArrayBuffer.empty[DenseVector[Double]]
    val weightbuf = ArrayBuffer.empty[DenseVector[Double]]
    val spweight_trans = transhape(spweight_dvec)
    val x_trans = transhape(x)
    for (i <- 0 until nlevel) { //i 是 第几类的类别循环，有几类循环几次
      for (groups <- _dataGroups.values) { //groups里面是每一组的情况，已经区分好了的
        //        for (j <- x.indices) { // j x的循环，x有多少列，循环多少次
        //          groups.foreach(t => {
        //            if (t._2 == i) {
        //              val tmp = x(j)(t._3)
        //              arrbuf += tmp
        //            }
        //          })
        //        }
        groups.foreach(t => {
          if (t._2 == i) {
            arrbuf += x_trans(t._3)
            weightbuf += spweight_trans(t._3)
          }
        })
        if(weightbuf.nonEmpty) {
          spweight_groups += weightbuf.toArray
        }
        if(arrbuf.nonEmpty) {
          _groupX += arrbuf.toArray
        }
        weightbuf.clear()
        arrbuf.clear()
      }
    }
  }

  //其实对于Array(Array)来说，直接用transpose就可以了，所以直接写了一个新的transhape
  def tranShape(target: Array[DenseVector[Double]]) : Array[DenseVector[Double]] = {
    val nrow = target.length
    val ncol = target(0).length
    val flat = target.flatMap(t => t.toArray)
    val flat_idx = flat.zipWithIndex
    val reshape = flat_idx.groupBy(t => {
      t._2 % ncol
    }).toArray
//    reshape.sortBy(_._1)//关键
    val result = reshape.sortBy(_._1).map(t => t._2.map(_._1))
    //对比
//    val mat1=DenseMatrix.create(nrow,ncol,flat)
//    val mat2=DenseMatrix.create(ncol,nrow,result.flatten)
//    println(mat1)
//    println("***trans***")
//    println(mat2)
    result.map(t => DenseVector(t))

  }

  def transhape(target: Array[DenseVector[Double]]) : Array[DenseVector[Double]] = {
    val tmp=target.map(t=>t.toArray).transpose
    tmp.map(t=>DenseVector(t))
  }

  def wqda()={

  }

  def wlda()={
    for(i<-0 until _groups) {
      val x1 = tranShape(_groupX(i))
      val w1 = tranShape(spweight_groups(i))
      getLocalMeanSigma(x1, w1)
      println(_groupX(i).length)
      val sumWeight = spweight_dvec.map(sum(_)).sum
//      println(sumWeight)
      val aLocalPrior = w1.map(t => {
        sum(t) / sumWeight
      })
      localPrior += DenseVector(aLocalPrior)
    }
//    println("------------SigmaGw-------------")
//    SigmaGw.foreach(t=>println(t.toVector))
//    println("++++++++++local mean++++++++++++")
//    localMean.foreach(t=>println(t.toList))
//    println("------------prior-------------")
//    localPrior.foreach(t=>println(t.toVector))

    getSigmai()

    println("========sigma used=========")
    sigma_wqda.foreach(println)
sigma_wlda.foreach(println)
//    val sigma = sigma_wlda.toArray
    val sigma = sigma_wqda.toArray
    sigma.foreach(println)

    val xt = _X.map(_.toArray).transpose.map(DenseVector(_))//x转置
//    xt.map(println(_))

    println("-------------localmean----------")
    localMean.foreach(t=>println(t.toList))

    for(i<-0 until _groups) {
      val arrPf = ArrayBuffer.empty[Double]
      for(j<-0 until _xrows){
        val lognorm = _groups / 2.0 * log(norm(sigma(j)))//wqda 单纯把sigma1换成SigmaGw
        val logprior= log(localPrior(i)(j))
        val meani=DenseVector(localMean.slice(i*_xcols,(i+1)*_xcols).map(t=>t(j)).toArray)
        val covmatj=DenseMatrix.create(_xcols,_xcols,sigma(j).toArray)
//        println(inv(covmatj))
        val pf=0.5 * (xt(j) - meani).t * inv(covmatj) * (xt(j) - meani)
        val logpf=lognorm + pf(0) - logprior
//        println(logpf)
        arrPf += logpf
      }
      groupPf += arrPf.toArray
      arrPf.clear()
    }
    println("------------log p result(different)---------")
    groupPf.foreach(t=>println(t.toVector))
    val groupPf_t = groupPf.toArray.transpose
    val maxProbIdx = groupPf_t.map(t => {
      t.indexWhere(_ == t.min)
    })
    println("maxProbIdx",maxProbIdx.toList)
    println(_distinctLevels.toList)
    val lev=maxProbIdx.map(t=>{
      figLevelString(t)
    })
    println("------------group predicted---------")
    println(lev.toList)
    validation(lev)
//    println(_levelArr.toVector)

    val np_ent=DenseVector.ones[Double](_xcols) / _xcols.toDouble
    val entMax = shannonEntropy(np_ent.toArray)
    val groupPf_t_exp=groupPf_t.map(t=>t.map(x=>Math.exp(-x)))
    val probs=groupPf_t_exp.map(t=>{
      t.map(x=> x / t.sum)
    })
    val pmax=probs.map(t=>t.max)
    val probs_shannon=probs.map(t=>shannonEntropy(t))
    val entropy=DenseVector(probs_shannon) / entMax
    println(s"entmax:$entMax")
    println("------------entropy---------")
    println(entropy)
    println("------------probs---------")
    probs.transpose.foreach(t=>println(t.toList))
    println("------------pmax---------")
    println(pmax.toVector)
  }

  def getLocalMeanSigma(x:Array[DenseVector[Double]], w: Array[DenseVector[Double]])= {
    val nlevel = _distinctLevels.length
    for (i <- x.indices) {
      val w_i = w.map(t => {
        val tmp = 1 / sum(t)
        t * tmp
      })
      val aLocalMean = w_i.map(w => w.t * x(i))
      //      for (j <- i until x.length) {
      for (j <- x.indices) {
        val aSigmaGw = w_i.map(t => {
          covwt(x(i), x(j), t)
        })
        SigmaGw += aSigmaGw
      }
      localMean += aLocalMean
    }
  }

  def getSigmai()={
    for(i<-0 until _xrows){
      var sigmaii= DenseVector.zeros[Double](_xcols*_xcols)
      for(j<-0 until _groups){
        val groupCounts = _groupX(j).length
        val group_sigmai = SigmaGw.map(_(i)).slice(j*_xcols*_xcols,(j+1)*_xcols*_xcols)
        sigma_wqda += DenseVector(group_sigmai.toArray)
        sigmaii = sigmaii + groupCounts.toDouble * DenseVector(group_sigmai.toArray)
      }
      sigma_wlda += (sigmaii / _xrows.toDouble)
//      println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
//      println(sigmaii / _xrows.toDouble)
    }
  }

  def figLevelString(rowi:Int): String= {
    var re = "NA"
    for (i <- _distinctLevels.indices) {
      if (rowi == _distinctLevels(i)._2) {
        re = _distinctLevels(i)._1.asInstanceOf[String]
      }
    }
    re
  }

  def figLevelInt(rowi: Int): Int = {
    var re = -1
    for (i <- _distinctLevels.indices) {
      if (rowi == _distinctLevels(i)._2) {
        re = _distinctLevels(i)._2
      }
    }
    re
  }

  private def covwt(x1: DenseVector[Double], x2: DenseVector[Double], w: DenseVector[Double]): Double = {
    val sqrtw = w.map(t => sqrt(t))
    val re1 = sqrtw * (x1 - sum(x1 * w))
    val re2 = sqrtw * (x2 - sum(x2 * w))
    val sumww = -sum(w.map(t => t * t)) + 1.0
    sum(re1 * re2 * (1 / sumww))
  }

  def shannonEntropy(data:Array[Double]):Double= {
    if (data.min < 0 || data.sum <= 0) {
      NaN
    }
    else {
      val pnorm = data.filter(_ > 0).map(_ / data.sum)
      pnorm.map { x =>
        -x * (Math.log(x) / Math.log(2))
      }.sum
    }
  }

//  def entropy(data: RDD[String]) = {
//    val size = data.count()
//    val p = data.map(x => (x, 1)).reduceByKey(_ + _).map {
//      case (value, num) => num.toDouble / size
//    }
//    p.map { x =>
//      -x * (Math.log(x) / Math.log(2))
//    }.sum
//  }

  def validation(predLev:Array[String]):Double={
    var nCorrect=0.0
    for(i<-predLev.indices){
      if(predLev(i)==_dataLevels(i)._1){
        nCorrect += 1
      }
    }
    println(s"correct ratio: ${nCorrect / _xrows}")
    nCorrect / _xrows
  }

}
