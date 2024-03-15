package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg
import breeze.linalg.{DenseMatrix, DenseVector, MatrixSingularException, inv, linspace, norm, sum}
import breeze.numerics.{NaN, log, sqrt}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize.goldenSelection

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GWDA extends GWRbase {

  val select_eps = 1e-6
  var _method: String = "wlda"
  var _inputY: Array[Any] = _
  var _distinctLevels: Array[_ <: (Any, Int)] = _
  var _levelArr: Array[Int] = _
  var _dataLevels: Array[(String, Int, Int)] = _ //Array是 (键，键对应的int值（从0开始计数），对应的索引位置)
  var _dataGroups: Map[String, Array[(String, Int, Int)]] = _
  var _nGroups: Int = 0
  var _groupX: ArrayBuffer[Array[DenseVector[Double]]] = ArrayBuffer.empty[Array[DenseVector[Double]]]
  //  var spweight_groups: ArrayBuffer[Array[DenseVector[Double]]] = ArrayBuffer.empty[Array[DenseVector[Double]]]  ////因为要反复用，所以不定义为全局变量
//  var groupPf: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]

  private var opt_value: Array[Double] = _
  private var opt_result: Array[Double] = _
  private var opt_iters: Array[Double] = _

  override def setY(property: String): Unit = {
    _nameY = property
    _inputY = shpRDD.map(t => t._2._2(property)).collect()
  }

  def calculate(bw: Double = 0, kernel: String = "bisquare", adaptive: Boolean = true)={
    if (bw > 0) {
      setweight(bw, kernel, adaptive)
    } else if (spweight_dvec != null) {

    } else {
      throw new IllegalArgumentException("bandwidth should be over 0 or spatial weight should be initialized")
    }


  }

  def bandwidthSelection(kernel: String = "bisquare", adaptive: Boolean = true, method :String = "wlda")= {
    _method = method
    var bwselect=0.0
    var printString = "Auto bandwidth selection\n"
    if (adaptive) {
      bwselect=adaptiveBandwidthSelection(kernel = kernel)
    } else {
      bwselect=fixedBandwidthSelection(kernel = kernel)
    }
    opt_iters.foreach(t => {
      val i = (t - 1).toInt
      printString += (f"iter ${t.toInt}, bandwidth: ${opt_value(i)}%.5f, correct ratio: ${opt_result(i)}%.5f\n")
    })
    println(printString)
    println(bwselect)
    bwselect
  }

  def bwSelectCriteria(bw: Double): Double = {
    setweight(bw, _kernel, _adaptive)
    val diag_weight0 = spweight_dvec.clone()
    for (i <- 0 until _xrows) {
      diag_weight0(i)(i) = 0
    }
    //    diagWeight0.foreach(t=>println(t))
    val group_weight = getWeightGroups(diag_weight0)
    _method match {
      case "wqda" => wqda()
      case "wlda" => wlda()
      case _ => throw new IllegalArgumentException("method should be wqda or wlda")
    }
  }

  private def fixedBandwidthSelection(kernel: String = "bisquare", upper: Double = max_dist, lower: Double = max_dist / 5000.0): Double = {
    _kernel = kernel
    _adaptive = false
    var bw: Double = lower
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, function = bwSelectCriteria)
      bw = re._1
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
    } catch {
      case e: MatrixSingularException => {
        val low = lower * 2
        bw = fixedBandwidthSelection(kernel, upper, low)
      }
    }
    bw
  }

  private def adaptiveBandwidthSelection(kernel: String = "bisquare", upper: Int = _xrows, lower: Int = 20): Int = {
    _kernel = kernel
    _adaptive = true
    var bw: Int = lower
    try {
      val re = goldenSelection(lower, upper, eps = select_eps, function = bwSelectCriteria)
      bw = re._1.toInt
      opt_iters = re._2
      opt_value = re._3
      opt_result = re._4
    } catch {
      case e: MatrixSingularException => {
        println("error")
        val low = lower + 1
        bw = adaptiveBandwidthSelection(kernel, upper, low)
      }
    }
    bw
  }

  def getYLevels(data: Array[_ <: Any] = _inputY) = {
    val data_idx = data.zipWithIndex
    val distin_idx = data.distinct.zipWithIndex
    val dlevel = data_idx.map(t => {
      distin_idx.find(_._1 == data(t._2))
    })
    //    println(dlevel.toVector)
    val levels = dlevel.map({
      getSomeInt(_)
    })
    //    println(levels.toVector)
    _distinctLevels = distin_idx
    _levelArr = levels
    _dataLevels = dlevel.zipWithIndex.map(t => ((getSomeStr(t._1), getSomeInt(t._1), t._2)))
    _dataGroups = _dataLevels.groupBy(_._1)
    _nGroups = _dataGroups.size
    //    levels
  }

  def getXGroups(x: Array[DenseVector[Double]] = _X) = {
    val nlevel = _distinctLevels.length
    val arrbuf = ArrayBuffer.empty[DenseVector[Double]]
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
          }
        })
        if (arrbuf.nonEmpty) {
          _groupX += arrbuf.toArray
        }
        arrbuf.clear()
      }
    }
  }

  def getWeightGroups(allWeight: Array[DenseVector[Double]] = spweight_dvec): Array[Array[DenseVector[Double]]] = {
    val nlevel = _distinctLevels.length
    val weightbuf = ArrayBuffer.empty[DenseVector[Double]]
    val spweight_trans = transhape(allWeight)
    val spweight_groups: ArrayBuffer[Array[DenseVector[Double]]] = ArrayBuffer.empty[Array[DenseVector[Double]]]
    for (i <- 0 until nlevel) {
      for (groups <- _dataGroups.values) {
        groups.foreach(t => {
          if (t._2 == i) {
            weightbuf += spweight_trans(t._3)
          }
        })
        if (weightbuf.nonEmpty) {
          spweight_groups += weightbuf.toArray
        }
        weightbuf.clear()
      }
    }
    spweight_groups.toArray
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


  //其实对于Array(Array)来说，直接用transpose就可以了，所以直接写了一个新的transhape
  def tranShape(target: Array[DenseVector[Double]]): Array[DenseVector[Double]] = {
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

  def transhape(target: Array[DenseVector[Double]]): Array[DenseVector[Double]] = {
    val tmp = target.map(t => t.toArray).transpose
    tmp.map(t => DenseVector(t))
  }

  def wqda():Double = {
    println("wqda")
    val sigmaGw: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty[Array[Array[Double]]]
    val localMean: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty[Array[Array[Double]]]
    val localPrior: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
    val spweight_groups = getWeightGroups(spweight_dvec)
    for (i <- 0 until _nGroups) {
      val x1 = tranShape(_groupX(i))
      val w1 = tranShape(spweight_groups(i))
      val re = getLocalMeanSigma(x1, w1)
      localMean += re._1
      sigmaGw += re._2
      val sumWeight = spweight_dvec.map(sum(_)).sum
      val aLocalPrior = w1.map(t => {
        sum(t) / sumWeight
      })
      localPrior += DenseVector(aLocalPrior)
    }

    val groupPf: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
    val sigma_wqda=sigmaGw.toArray.map(t=>t.transpose.map(DenseVector(_)))
    val xt = _X.map(_.toArray).transpose.map(DenseVector(_)) //x转置
    for (i <- 0 until _nGroups) {
      val meani = localMean(i).transpose.map(DenseVector(_))
      val arrPf = ArrayBuffer.empty[Double]
      for (j <- 0 until _xrows) {
        val lognorm = _nGroups / 2.0 * log(norm(sigma_wqda(i)(j)))
        val logprior = log(localPrior(i)(j))
        val covmatj = DenseMatrix.create(_xcols, _xcols, sigma_wqda(i)(j).toArray)
        val pf = 0.5 * (xt(j) - meani(j)).t * inv(covmatj) * (xt(j) - meani(j))
        val logpf = lognorm + pf(0) - logprior
        arrPf += logpf
      }
      groupPf += arrPf.toArray
      arrPf.clear()
    }
    println("------------log p result---------")
    groupPf.foreach(t => println(t.toVector))
    val groupPf_t = groupPf.toArray.transpose
    val minProbIdx = groupPf_t.map(t => {
      t.indexWhere(_ == t.min)
    })
    println("minProbIdx", minProbIdx.toList)
    println(_distinctLevels.toList)
    val lev = minProbIdx.map(t => {
      figLevelString(t)
    })
    println("------------group predicted---------")
    println(lev.toList)
//    summary(groupPf_t)
    validation(lev)
  }

  def wlda() = {

//    println("wlda")
    val sigmaGw: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty[Array[Array[Double]]]
    val localMean: ArrayBuffer[Array[Array[Double]]] = ArrayBuffer.empty[Array[Array[Double]]]
    val localPrior: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
    val spweight_groups = getWeightGroups(spweight_dvec)
    for (i <- 0 until _nGroups) {
      val x1 = tranShape(_groupX(i))
      val w1 = tranShape(spweight_groups(i))
      val re=getLocalMeanSigma(x1, w1)
      localMean+=re._1
      sigmaGw+=re._2
//      println(_groupX(i).length)
      val sumWeight = spweight_dvec.map(sum(_)).sum
      val aLocalPrior = w1.map(t => {
        sum(t) / sumWeight
      })
      localPrior += DenseVector(aLocalPrior)
    }

//        println("------------SigmaGw-------------")
//        SigmaGw.foreach(t=>println(t.toVector))
//        println("++++++++++local mean++++++++++++")
//        localMean.foreach(t=>println(t.toList))
//        println("------------prior-------------")
//        localPrior.foreach(t=>println(t.toVector))
//        println("========sigma used(sigma1)=========")
//        sigma_wlda.foreach(println)
//        println("-------------localmean----------")
//        localMean.foreach(t => println(t.toList))

    val groupPf: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
    val sigmaWlda=getSigmai(sigmaGw.toArray)
    val sigma_wlda=sigmaWlda.map(t=>DenseVector(t))
    val xt = _X.map(_.toArray).transpose.map(DenseVector(_)) //x转置
    for (i <- 0 until _nGroups) {
      val arrPf = ArrayBuffer.empty[Double]
      val meani = localMean(i).transpose.map(DenseVector(_))
      for (j <- 0 until _xrows) {
        val lognorm = _nGroups / 2.0 * log(norm(sigma_wlda(j)))
        val logprior = log(localPrior(i)(j))
        val covmatj = DenseMatrix.create(_xcols, _xcols, sigma_wlda(j).toArray)
        val pf = 0.5 * (xt(j) - meani(j)).t * inv(covmatj) * (xt(j) - meani(j))
        val logpf = lognorm + pf(0) - logprior
        arrPf += logpf
      }
      groupPf += arrPf.toArray
      arrPf.clear()
    }
//    println("------------log p result---------")
//    groupPf.foreach(t => println(t.toVector))
    val groupPf_t = groupPf.toArray.transpose
    val minProbIdx = groupPf_t.map(t => {
      t.indexWhere(_ == t.min)
    })
//    println("minProbIdx", minProbIdx.toList)
//    println(_distinctLevels.toList)
    val lev = minProbIdx.map(t => {
      figLevelString(t)
    })
//    println("------------group predicted---------")
//    println(lev.toList)
//    summary(groupPf_t)
    validation(lev)
  }

  def summary(groupPf_t: Array[Array[Double]])={
    val np_ent = DenseVector.ones[Double](_xcols) / _xcols.toDouble
    val entMax = shannonEntropy(np_ent.toArray)
    val groupPf_t_exp = groupPf_t.map(t => t.map(x => Math.exp(-x)))
    val probs = groupPf_t_exp.map(t => {
      t.map(x => x / t.sum)
    })
    val pmax = probs.map(t => t.max)
    val probs_shannon = probs.map(t => shannonEntropy(t))
    val entropy = DenseVector(probs_shannon) / entMax
    println(s"entmax:$entMax")
    println("------------entropy---------")
    println(entropy)
    println("------------probs---------")
    probs.transpose.foreach(t => println(t.toList))
    println("------------pmax---------")
    println(pmax.toVector)

  }

  def getLocalMeanSigma(x: Array[DenseVector[Double]], w: Array[DenseVector[Double]]): (Array[Array[Double]], Array[Array[Double]]) = {
    val sigmaGw: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
    val localMean: ArrayBuffer[Array[Double]] = ArrayBuffer.empty[Array[Double]]
    val nlevel = _distinctLevels.length
    for (i <- x.indices) {
      val w_i = w.map(t => {
        val tmp = 1 / sum(t)
        t * tmp
      })
      val aLocalMean = w_i.map(w => w.t * x(i))
      for (j <- x.indices) {
        val aSigmaGw = w_i.map(t => {
          covwt(x(i), x(j), t)
        })
        sigmaGw += aSigmaGw
      }
      localMean += aLocalMean
    }
    (localMean.toArray, sigmaGw.toArray)
  }

  def getSigmai(SigmaGw: Array[Array[Array[Double]]]): Array[Array[Double]] = {
    val sigmaWlda: ArrayBuffer[DenseVector[Double]] = ArrayBuffer.empty[DenseVector[Double]]
    for (i <- 0 until _xrows) {
      var sigmaii = DenseVector.zeros[Double](_xcols * _xcols)
      for (j <- 0 until _nGroups) {
        val groupCounts = _groupX(j).length
        val group_sigmai = SigmaGw(j).transpose
        sigmaii = sigmaii + groupCounts.toDouble * DenseVector(group_sigmai(i))
      }
      sigmaWlda += (sigmaii / _xrows.toDouble)
    }
    sigmaWlda.map(t=>t.toArray).toArray
  }

  def figLevelString(rowi: Int): String = {
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

  def shannonEntropy(data: Array[Double]): Double = {
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

  def validation(predLev: Array[String]): Double = {
    var nCorrect = 0.0
    for (i <- predLev.indices) {
      if (predLev(i) == _dataLevels(i)._1) {
        nCorrect += 1
      }
    }
    println(s"correct ratio: ${nCorrect / _xrows}")
    nCorrect / _xrows
  }

}
