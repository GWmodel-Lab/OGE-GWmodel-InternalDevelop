package whu.edu.cn.algorithms.SpatialStats.SpatialHeterogeneity

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.oge.Feature

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import breeze.stats
import breeze.linalg
import org.apache.commons.math3.special.Beta
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.distribution.FDistribution


object Geodetector {

  /**
   *
   * @param featureRDD
   * @param y_title
   * @param x_titles
   * @param is_print
   * @return 包括变量名、q值和p值
   */
  def factorDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                     y_title: String, x_titles: List[String], is_print: Boolean = true)
  : (List[String], List[Double], List[Double]) = {
    val y_col = Feature.get(featureRDD, y_title).map(t=>t.toString.toDouble)
    val x_cols = x_titles.map(t => Feature.get(featureRDD, t))
    val QandP = x_cols.map(t => FD_single(y_col, t))
    val res = (x_titles, QandP.map(t => t._1), QandP.map(t => t._2))
    if (is_print) {
      FD_print(res)
    }
    res
  }

  protected def FD_single(y_col: List[Double], x_col: List[Any]): (Double, Double) = {
    val y_grouped = Grouping(y_col, x_col)
    val SST = getDev(y_col) * y_col.length
    val SSW = y_grouped.map(t => t.length * getDev(t)).sum
    val q = 1 - SSW / SST
    val sum_sq_mean = y_grouped.map(t => Math.pow(stats.mean(t), 2)).sum
    val sq_sum = y_grouped.map(t => Math.sqrt(t.length) * stats.mean(t)).sum
    val N_sample: Double = y_col.length
    val N_strata: Double = y_grouped.length
    val ncp = (sum_sq_mean - sq_sum * sq_sum / N_sample) / stats.variance(y_col)
    val f_val: Double = (N_sample - N_strata) / (N_strata - 1) * (q / (1 - q))
    val p = 1 - noncentralFCDF(f_val, df1 = N_strata - 1, df2 = N_sample - N_strata, ncp)

    (q, p)
  }

  protected def FD_print(res: (List[String], List[Double], List[Double])): Unit = {
    println("--------------------------Results of Factor Detector--------------------------------")
    val len = res._1.length
    var no = 1
    for (i <- 0 until len) {
      println(f"variable $no%-2d: ${res._1(i)}%-10s, q: ${res._2(i)}%-1.5f, p: ${res._3(i)}%-5g")
      no += 1
    }
    println()
  }

  /**
   *
   * @param featureRDD
   * @param y_title
   * @param x_titles
   * @param is_print
   * @return 自变量名、q值（矩阵）、增强类型（矩阵）
   */
  def interactionDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                          y_title: String, x_titles: List[String], is_print: Boolean = true)
  : (List[String], linalg.Matrix[Double], linalg.Matrix[String]) = {
    val y_col = Feature.get(featureRDD, y_title).map(t=>t.toString.toDouble)
    val x_cols = x_titles.map(t => Feature.get(featureRDD, t))
    var interactions = linalg.Matrix.zeros[Double](x_titles.length, x_titles.length)
    var enhancement = linalg.Matrix.zeros[String](x_titles.length, x_titles.length)
    for (i <- 0 until x_titles.length) {
      for (j <- 0 until i) {
        val q = ID_single(y_col, x_cols(i), x_cols(j))
        val q1 = getQ(y_col, x_cols(i))
        val q2 = getQ(y_col, x_cols(j))
        val q_min = Math.min(q1, q2)
        val q_max = Math.max(q1, q2)
        interactions(i, j) = q
        interactions(j, i) = q
        if (q < q_min) {
          enhancement(i, j) = "Weakened, nonlinear"
        }
        else if (q > q_min && q < q_max) {
          enhancement(i, j) = "Weakened, uni-"
        }
        else {
          if (q > q1 + q2) {
            enhancement(i, j) = "Enhanced, Nonlinear"
          }
          else if (q == q1 + q2) {
            enhancement(i, j) = "Independent"
          }
          else {
            enhancement(i, j) = "Enhanced, bi-"
          }
        }
      }
    }
    val res = (x_titles, interactions, enhancement)
    if (is_print) {
      ID_print(res)
    }
    res
  }

  protected def ID_single(y_col: List[Double], x_col1: List[Any], x_col2: List[Any]): Double = {
    var interaction: ListBuffer[Any] = ListBuffer(List("0", "0"))
    for (i <- 0 until x_col1.length) {
      interaction.append(List(x_col1(i), x_col2(i)))
    }
    val q = getQ(y_col, interaction.drop(1).toList)
    q
  }

  protected def ID_print(res: (List[String], linalg.Matrix[Double], linalg.Matrix[String])): Unit = {
    println("--------------------------Results of Interaction Detector--------------------------------")
    //printMatrixWithTitles_Double((res._1,res._2))
    //printMatrixWithTitles_String((res._1,res._3))
    val N_var = res._1.length
    var no = 1
    for (i <- 0 until N_var) {
      for (j <- 0 until i) {
        println(f"interaction $no, variable1: ${res._1(i)}%-10s, variable2: ${res._1(j)}%-10s, q: ${res._2(i, j)}%-1.5f, sig: ${res._3(i, j)}%s")
        no += 1
      }
    }
    println()
  }

  /**
   *
   * @param featureRDD
   * @param y_title
   * @param x_titles
   * @param is_print
   * @return 变量名、是否显著（矩阵）
   */
  def ecologicalDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                         y_title: String, x_titles: List[String], is_print: Boolean = true)
  : (List[String], linalg.Matrix[Boolean]) = {
    val y_col = Feature.get(featureRDD, y_title).map(t=>t.toString.toDouble)
    val x_cols = x_titles.map(t => Feature.get(featureRDD, t))
    //val F_mat = linalg.Matrix.zeros[Double](x_titles.length, x_titles.length) // Matrix of Statistic F
    val Sig_mat = linalg.Matrix.zeros[Boolean](x_titles.length, x_titles.length) // Matrix of True or False
    for (i <- 0 until x_titles.length) {
      for (j <- 0 until x_titles.length) {
        val F_val = ED_single(y_col, x_cols(i), x_cols(j))
        //F_mat(i, j) = F_val
        //F-test
        val Nx1 = x_cols(i).length
        val Nx2 = x_cols(j).length
        val F = new FDistribution((Nx1 - 1), (Nx2 - 1))
        Sig_mat(i, j) = F_val > F.inverseCumulativeProbability(0.9)
      }
    }
    val res = (x_titles, Sig_mat)
    if (is_print) {
      ED_print(res)
    }
    res
  }

  protected def ED_single(y_col: List[Double], x_col1: List[Any], x_col2: List[Any]): Double = {
    val f_numerator = getQ(y_col, x_col2)
    val f_denominator = getQ(y_col, x_col1)
    val res = f_numerator / f_denominator
    breeze.linalg.max(res, 1 / res)
  }

  protected def ED_print(res: (List[String], linalg.Matrix[Boolean])): Unit = {
    println("--------------------------Results of Ecological Detector--------------------------------")
    printMatrixWithTitles_Boolean(res)
    println()
  }

  /**
   *
   * @param featureRDD
   * @param y_title
   * @param x_titles
   * @return 变量名、各变量的子区域、各变量子区域对应的y均值，各变量不同子区域之间的显著性
   */
  def riskDetector(featureRDD: RDD[(String, (Geometry, Map[String, Any]))],
                   y_title: String, x_titles: List[String], is_print: Boolean =true):
  (List[String], List[List[String]], List[List[Double]], List[linalg.Matrix[Boolean]]) = {
    val y_col = Feature.get(featureRDD, y_title).map(t=>t.toString.toDouble)
    val x_cols = x_titles.map(t => Feature.get(featureRDD, t))
    val lst1 = ListBuffer("")
    val lst2 = ListBuffer(List(""))
    val lst3 = ListBuffer(List(0.0))
    val lst4 = ListBuffer(linalg.Matrix.zeros[Boolean](1, 1))
    for (i <- 0 until x_cols.length) {
      val res = RD_single(y_col, x_cols(i))
      lst1.append(x_titles(i))
      lst2.append(res._1)
      lst3.append(res._2)
      lst4.append(res._3)
    }
    val res = (lst1.drop(1).toList, lst2.drop(1).toList, lst3.drop(1).toList, lst4.drop(1).toList)
    if(is_print){
     RD_print(res)
    }
    res
  }

  protected def RD_single(y_col: List[Double], x_col: List[Any]):
  (List[String], List[Double], linalg.Matrix[Boolean]) = {
    val groupXY = GroupingXAndY(y_col, x_col)
    val groupY = groupXY.map(t => t._2)
    val groupX = groupXY.map(t => t._1)
    val N_strata = groupY.length
    val means_variable = groupY.map(t => stats.mean(t))
    val T_Mat = linalg.Matrix.zeros[Boolean](N_strata, N_strata)
    // val T_Mat1 = linalg.Matrix.zeros[Int](N_strata, N_strata)
    for (i <- 0 until N_strata) {
      for (j <- 0 until N_strata) {
        val group1 = groupY(i)
        val group2 = groupY(j)
        val mean1 = stats.mean(group1)
        val mean2 = stats.mean(group2)
        val var1 = stats.variance(group1)
        val var2 = stats.variance(group2)
        val n1 = group1.length
        val n2 = group2.length
        val t_numerator = mean1 - mean2
        val t_denominator = math.sqrt(var1 / n1 + var2 / n2)
        val t_val = (t_numerator / t_denominator).abs
        // Test
        // T_Mat1(i, j) = -1
        if (n1 > 1 && n2 > 1) {
          val df_numerator = math.pow(var1 / n1 + var2 / n2, 2)
          val df_denominator = math.pow(var1 / n1, 2) / (n1 - 1) + math.pow(var2 / n2, 2) / (n2 - 1)
          if (df_denominator != 0) {
            val df_val = df_numerator / df_denominator
            T_Mat(i, j) = new TDistribution(df_val).cumulativeProbability(t_val) > 0.975
            //  if (T_Mat(i, j)) {
            //    T_Mat1(i, j) = 1
            //  }
            //  else T_Mat1(i, j) = 0
            /* 1 means true, 0 means false, -1 means incapable to compute */
          }
        }
      }
    }
    (groupX, means_variable, T_Mat)
  }

  protected def RD_print(res: (List[String], List[List[String]], List[List[Double]], List[linalg.Matrix[Boolean]])):
  Unit ={
    println("--------------------------Result of Risk Detector--------------------------------")
    val N_var = res._1.length
    for(no <- 0 until N_var){
      println(f"Variable: ${res._1(no)}")
      println("-------------------------")
      println(f"Means of Strata: ")
      for(i <- 0 until res._2(no).length){
        println(f"stratum ${i+1}: ${res._2(no)(i)}, mean: ${res._3(no)(i)}%10f")
      }
      println("-------------------------")
      println(f"Significance: ")
      printMatrixWithTitles_Boolean((res._2(no),res._4(no)))
      println()
    }

  }


  protected def Grouping(y_col: List[Double], x_col: List[Any]): List[List[Double]] = {
    if (y_col.length != x_col.length) {
      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${y_col.length} while size of x is ${x_col.length}.")
    }
    val list_xy: ListBuffer[Tuple2[Any, Double]] = ListBuffer(("", 0.0))
    for (i <- 0 until y_col.length) {
      list_xy.append((x_col(i), y_col(i)))
    }
    val sorted_xy = list_xy.drop(1).toList.groupBy(_._1).mapValues(r => r.map(r => {
      r._2
    })).map(r => r._2)
    sorted_xy.toList
  }

  protected def GroupingXAndY(y_col: List[Double], x_col: List[Any]): List[Tuple2[String, List[Double]]] = {
    if (y_col.length != x_col.length) {
      throw new IllegalArgumentException(s"The sizes of the y and x are not equal, size of y is ${y_col.length} while size of x is ${x_col.length}.")
    }
    val list_xy: ListBuffer[Tuple2[String, Double]] = ListBuffer(("", 0.0))
    for (i <- 0 until y_col.length) {
      list_xy.append((x_col(i).toString, y_col(i)))
    }
    val sorted_xy = list_xy.drop(1).sortBy(_._1).toList.groupBy(_._1).mapValues(r => r.map(r => {
      r._2
    }))
    sorted_xy.toList.sortBy(_._1)
  }

  protected def getDev(list: List[Double]): Double = {
    val mean: Double = list.sum / list.length
    var res: Double = 0
    for (i <- 0 until list.length) {
      res = res + (mean - list(i)) * (mean - list(i))
    }
    res / list.length
  }

  protected def getQ(y_col: List[Double], x_col: List[Any]): Double = {
    val y_grouped = Grouping(y_col, x_col)
    val SST = getDev(y_col) * y_col.length
    val SSW = y_grouped.map(t => t.length * getDev(t)).sum
    val q = 1 - SSW / SST
    q
  }

  protected def noncentralFCDF(x: Double, df1: Double, df2: Double, noncentrality: Double,
                               maxIterations: Int = 2000, tolerance: Double = 1e-9): Double = {
    var result = 0.0
    var k = 0
    var term = 0.999
    while (k < maxIterations) {
      /*
      power = math.pow(noncentrality * 0.5, k)
      fact = factorial(k)
      powerDividedByFactorial = power/fact
      这里不直接求幂和阶乘，防止值过大导致计算错误
       */
      var powerDividedByFactorial = 1.0
      if(k==0){
        powerDividedByFactorial=1
      }
      else{
        for (i<-0 until k){
          powerDividedByFactorial *= noncentrality/(2*(i+1))
        }
      }
      val expo = math.exp(-0.5 * noncentrality)
      val beta = Beta.regularizedBeta(df1 * x / (df2 + df1 * x), df1 / 2.0 + k, df2 / 2.0)
      term = powerDividedByFactorial * expo * beta
      //println((powerDividedByFactorial, expo, beta))
      //print(f"${k+1} ")
      //println(term)
      if (math.abs(term) > tolerance && math.abs(term) < 1) {
        result += term
      }
      k += 1
    }
    if (result < 0) {
      result = 0
    }
    else if (result > 1) {
      result = 1
    }
    result
  }

  protected def printMatrixWithTitles_Boolean(res: (List[String], linalg.Matrix[Boolean])): Unit = {
    val len = res._1.length
    val mat_print = linalg.Matrix.zeros[String](len + 1, len + 1)
    for (i <- 0 until len + 1) {
      for (j <- 0 until len + 1) {
        if (i == 0 && j == 0) {}
        else if (i == 0) {
          mat_print(i, j) = res._1(j - 1)
        } else if (j == 0) {
          mat_print(i, j) = res._1(i - 1)
        } else {
          mat_print(i, j) = res._2(i - 1, j - 1).toString
        }
      }
    }
    println(mat_print)
  }

}
