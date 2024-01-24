package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{DenseMatrix, DenseVector}

import scala.collection.mutable.ArrayBuffer

class GWDA extends GWRbase {

  var _distinctLevels: Array[_ <: (Any, Int)]= _
  var _levels: Array[Int] = _
  var _dataLevels: Array[(String,Int,Int)] = _ //Array是 键，键对应的int值（从0开始计数），对应的索引位置
  var _dataGroups: Map[String, Array[(String, Int, Int)]] = _
  var _groupX:ArrayBuffer[DenseMatrix[Double]] = ArrayBuffer.empty[DenseMatrix[Double]]
  var spweight_groups: ArrayBuffer[Array[DenseVector[Double]]] = ArrayBuffer.empty[Array[DenseVector[Double]]]

  def wqdaCr(bw:Double, kernel: String = "gaussian", adaptive: Boolean = true)={
    setweight(bw, kernel, adaptive)
//    val spweightIdx=spweight_dvec.zipWithIndex
//    spweightIdx.foreach(t=> {
//      spweight_dvec.foreach(w=>w(t._2)=0)
//    })
    for(i<-0 to _xrows){
      spweight_dvec.map(t=>t(i)=0)
    }
    spweight_dvec.foreach(t=>println(t))
  }

  def getLevels(data: Array[_ <: Any]): Array[Int]={
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
    _levels=levels
    _dataLevels=dlevel.zipWithIndex.map(t=>((getSomeStr(t._1),getSomeInt(t._1),t._2)))
//    println(_dataLevels.toVector)
    _dataGroups=_dataLevels.groupBy(_._1)
//    _dataGroups.foreach(println(_))
//    for(i<-_dataGroups.values){
//      println(i.toVector)
//    }
//    val xizang=_dataGroups("西藏自治区")
//    println(xizang.toVector)
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
    //    val arrbuf = ArrayBuffer.empty[DenseVector[Double]]
    val arrbuf = ArrayBuffer.empty[Double]
    val weightbuf = ArrayBuffer.empty[DenseVector[Double]]
    val spweight_trans = tranShape(spweight_dvec)
    //    val x_trans=tranShape(x)
    for (i <- 0 until nlevel - 1) { //i 是 第几类的类别循环，有几类循环几次
      for (groups <- _dataGroups.values) { //groups里面是每一组的情况，已经区分好了的
        for (j <- x.indices) { // j x的循环，x有多少列，循环多少次
          groups.foreach(t => {
            if (t._2 == i) {
              val tmp = x(j)(t._3)
              arrbuf += tmp
            }
          })
        }
        groups.foreach(t => {
          if (t._2 == i) {
            //            arrbuf += x_trans(t._3)
            weightbuf += spweight_trans(t._3)
          }
        })
        spweight_groups += weightbuf.toArray
        weightbuf.clear()
      }
      //      val arrtmp=arrbuf.map(t=>t.toArray).toArray

      val arrtmp = arrbuf.toArray
      //      val tmpmat = DenseMatrix.create(rows = arrtmp.length / _xcols, cols = _xcols, data = arrtmp)
      //      println(tmpmat)
      _groupX += DenseMatrix.create(rows = arrtmp.length / _xcols, cols = _xcols, data = arrtmp)
      arrbuf.clear()
    }
    _groupX.foreach(println(_))
    spweight_groups.foreach(t => t.foreach(println(_)))
  }

  def tranShape(target: Array[DenseVector[Double]]) : Array[DenseVector[Double]] = {
    val nrow = target.length
    val ncol = target(0).length
    val flat = target.flatMap(t => t.toArray)
    val flat_idx = flat.zipWithIndex
    val reshape = flat_idx.groupBy(t => {
      t._2 % nrow
    }).toArray
//    reshape.foreach(println)
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

  def wqda()={

  }

  def wlda(x:Array[DenseVector[Double]], grouping: Array[Int], xpr:Array[DenseVector[Double]], w:  Array[DenseVector[Double]]=spweight_dvec)={
    val nlevel=_distinctLevels.length


  }


}
