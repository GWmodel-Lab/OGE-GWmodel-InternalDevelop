package whu.edu.cn.algorithms.SpatialStats.STSampling

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import breeze.stats
import breeze.linalg
import org.apache.commons.math3
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.distribution.FDistribution
import breeze.linalg.DenseVector

import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import whu.edu.cn.oge.Feature._
import whu.edu.cn.util.ShapeFileUtil._
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils._

object SandwichSampling {

  /**
   *
   * @param featureRDD
   * @param y_title
   * @param knowledge_title
   * @param reporting_title
   * @param accuracy
   * @return 返回RDD，即最终抽取的样本
   */
  def sampling(featureRDD: RDD[(String, (Geometry, Map[String, Any]))], sc: SparkContext,
               y_title: String, knowledge_title: String, reporting_title: String, accuracy: Double = 0.05):
  RDD[(String, (Geometry, Map[String, Any]))] = {
    // test error
    if (accuracy > 1.0 || accuracy < 0.0) {
      throw new Exception("抽样精度必须在0到1之间。")
    }

    val oriVals = get(featureRDD, y_title).map(t => t.toString.toDouble)
    val oriOrderNum = (0 until oriVals.length).toList // 序号
    val oriKnowledge = Feature.getString(featureRDD, knowledge_title)
    val oriReport = Feature.getString(featureRDD, reporting_title)

    // 根据Cochran的方法计算样本数
    val t_alpha = 2.5758 // 置信度0.99下的标准正态偏差
    val std = stats.stddev(oriVals)
    val d = stats.mean(oriVals) * accuracy
    val sampleSizeTotal = math.pow((t_alpha * std) / d, 2).toInt
    val df: ListBuffer[(Int, Double, String, String)] = ListBuffer.empty[(Int, Double, String, String)]
    for (i <- 0 until oriVals.length) {
      df.append((i, oriVals(i), oriKnowledge(i), oriReport(i)))
    }

    // 根据知识层进行分类
    val df_groupedByKnowledge = Grouping(df.toList, oriKnowledge) // 根据知识层对df进行分层
    val orderNum_groupedByKnowledge = GroupingInt(oriOrderNum, oriKnowledge)
    var sampleSizeKnowledge = df_groupedByKnowledge.map(t => math.round(t.length.toDouble * sampleSizeTotal / oriVals.length).toInt)
    sampleSizeKnowledge = sampleSizeKnowledge.map(t => {
      if (t == 0) {
        1 // 确保每层至少一个样本
      }
      else {
        t
      }
    })

    /*---------------------------------Sampling--------------------------------------------*/
    //抽样样本的序号，保留两层（知识层和层内的样本），便于后续处理
    val sampleOrderNumKnowledge: ListBuffer[List[Int]] = ListBuffer.empty[List[Int]]
    for (i <- 0 until orderNum_groupedByKnowledge.length) {
      val s = Random.shuffle(orderNum_groupedByKnowledge(i)).take(sampleSizeKnowledge(i)) //层内抽样
      sampleOrderNumKnowledge.append(s)
    }

    // 抽样生成新的RDD
    //val t0 = System.currentTimeMillis()
    //val res: ListBuffer[(Int, Double, String, String)] = ListBuffer.empty[(Int, Double, String, String)]
    type typeRddElement = (String, (Geometry, Map[String, Any]))
    val res1: ArrayBuffer[typeRddElement] = ArrayBuffer.empty[typeRddElement]
    val featureArray = featureRDD.collect()
    for (i <- 0 until sampleOrderNumKnowledge.length) {
      for (j <- 0 until sampleOrderNumKnowledge(i).length) {
        val no = sampleOrderNumKnowledge(i)(j)
        //res.append(df(no))
        res1.append(featureArray(no))
      }
    }
    //println(s"t = ${(System.currentTimeMillis() - t0) / 1000.0} s.")


    /*-------------------------计算抽样结果指标------------------------*/
    val sampleVals = sampleOrderNumKnowledge.map(t => t.map(t => df(t)._2))
    val meansKnowledge = sampleVals.map(t => stats.mean(t)) //yZbar
    val varsKnowledge = sampleVals.map(t => stats.variance(t))
    val sizeKnowledge = sampleVals.map(t => t.length)
    val var_meansKnowledge = stats.variance(meansKnowledge.toList) //V_yZbar
    val sampleVals_flat: ListBuffer[Double] = ListBuffer.empty[Double] // no grouped
    for (i <- 0 until sampleVals.length) {
      for (j <- 0 until sampleVals(i).length) {
        sampleVals_flat.append(sampleVals(i)(j))
      }
    }

    val sampleKnowledge = sampleOrderNumKnowledge.map(t => t.map(t => df(t)._3))
    val namesKnowledge = sampleKnowledge.map(t => t.distinct(0)) //知识层各层名称
    val namesReport = oriReport.distinct //报告层各层名称

    //计算各报告层样本量
    val sampleReport = sampleOrderNumKnowledge.map(t => t.map(t => df(t)._4))
    val sampleReport_flat: ListBuffer[String] = ListBuffer.empty[String] // no grouped
    for (i <- 0 until sampleReport.length) {
      for (j <- 0 until sampleReport(i).length) {
        sampleReport_flat.append(sampleReport(i)(j))
      }
    }
    var sampleSizeReport = sampleReport_flat.groupBy(identity).mapValues(_.length)

    //计算报告层与知识层相交后，各子区内的样本量
    val sampleZoneAndReport = sampleOrderNumKnowledge.map(t => t.map(t => (df(t)._3, df(t)._4)))
    var sampleZR_flat: ListBuffer[(String, String)] = ListBuffer.empty[(String, String)]
    for (i <- 0 until sampleZoneAndReport.length) {
      for (j <- 0 until sampleZoneAndReport(i).length) {
        sampleZR_flat.append(sampleZoneAndReport(i)(j))
      }
    }
    var sampleSzZR = sampleZR_flat.groupBy(identity).mapValues(_.length)
    var matSzZR = linalg.Matrix.zeros[Int](namesKnowledge.length, namesReport.length)
    for (i <- 0 until namesKnowledge.length) {
      for (j <- 0 until namesReport.length) {
        var key0 = (namesKnowledge(i), namesReport(j))
        //println(key0)
        if (sampleSzZR.contains(key0)) {
          matSzZR(i, j) = sampleSzZR(key0)
        }
      }
    }

    // 正式计算报告层均值与方差
    var meansReport: ListBuffer[Double] = ListBuffer.empty[Double]
    var varsReport: ListBuffer[Double] = ListBuffer.empty[Double]
    for (i <- 0 until namesReport.length) {
      var yRbar = 0.0
      var V_yRbar = 0.0
      val Nr = sampleSizeReport(namesReport(i))
      //println(s"Nr is $Nr")
      for (j <- 0 until namesKnowledge.length) {
        val Nrz = matSzZR(j, i)
        val yZbar = meansKnowledge(j)
        yRbar += Nrz.toDouble * yZbar / Nr
        V_yRbar += math.pow(Nrz.toDouble / Nr, 2) * var_meansKnowledge
      }
      meansReport.append(yRbar)
      varsReport.append(V_yRbar)
    }


    println("-----------------三明治抽样报告------------------")
    println(
      f"""|知识层: ${knowledge_title}%-10s; 层数: ${namesKnowledge.length}
          |报告层: ${reporting_title}%-10s; 层数: ${namesReport.length}
          |检测属性: $y_title
          |总体样本量: ${oriVals.length}
          |期望精度: ${accuracy}
          |应抽样本量: ${sampleSizeTotal}
          |实抽样本量: ${res1.length}""".stripMargin)
    println("-------------------知识层参数-------------------")
    println(
      f"""|均值方差: ${stats.variance(meansKnowledge)}%-2.3f
          |均值空间标准差: ${stats.stddev(meansKnowledge)}%-2.3f
          |均值上限: ${meansKnowledge.max}%-2.3f
          |均值下限: ${meansKnowledge.min}%-2.3f """.stripMargin
    )
    println("知识层     均值        方差       样本量")
    for (i <- 0 until meansKnowledge.length) {
      println(f"${namesKnowledge(i)}%5s     ${meansKnowledge(i)}%2.3f     ${varsKnowledge(i)}%2.3f     ${sizeKnowledge(i)}")
    }
    // println("NO       Value     Zoning     Reporting")
    // for (i <- 0 until res.length) {
    //   println(f"${res(i)._1}%4s     ${res(i)._2}%-2.5f    ${res(i)._3}%-5s     ${res(i)._4}%-5s")
    //   //println(res1(i))
    // }
    println("----------各报告层及其参数----------")
    println("报告层     均值        方差       样本量")
    for (i <- 0 until meansReport.length) {
      println(f"${namesReport(i)}%5s     ${meansReport(i)}%-2.3f     ${varsReport(i)}%-2.3f     ${sampleSizeReport(namesReport(i))}")
    }
    println("-------------报告结束-------------")

    //output
    sc.makeRDD(res1)
  }

  protected def Grouping(y_col: List[Any], x_col: List[Any])
  : List[List[Any]] = {
    if (y_col.length != x_col.length) {
      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${y_col.length} while size of x is ${x_col.length}.")
    }

    val list_xy: ListBuffer[Tuple2[Any, Any]] =
      ListBuffer(("", List(0, 0.0, "", "")))
    for (i <- 0 until y_col.length) {
      list_xy.append((x_col(i), y_col(i)))
    }
    val sorted_xy = list_xy.drop(1).toList.groupBy(_._1).mapValues(r => r.map(r => {
      r._2
    })).map(r => r._2)
    sorted_xy.toList
  }

  protected def GroupingInt(y_col: List[Int], x_col: List[Any])
  : List[List[Int]] = {
    if (y_col.length != x_col.length) {
      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${y_col.length} while size of x is ${x_col.length}.")
    }

    val list_xy: ListBuffer[Tuple2[Any, Int]] =
      ListBuffer(("", 0))
    for (i <- 0 until y_col.length) {
      list_xy.append((x_col(i), y_col(i)))
    }
    val sorted_xy = list_xy.drop(1).toList.groupBy(_._1).mapValues(r => r.map(r => {
      r._2
    })).map(r => r._2)
    sorted_xy.toList
  }
}
