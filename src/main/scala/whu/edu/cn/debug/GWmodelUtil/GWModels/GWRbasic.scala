package whu.edu.cn.debug.GWmodelUtil.GWModels

import breeze.linalg.{DenseMatrix, DenseVector, eig, inv, qr, sum}
import breeze.numerics.sqrt
import scala.math._

class GWRbasic extends GWRbase {

  var _xrows = 0
  var _xcols = 0
  private var _df = _xcols

  private var _dX: DenseMatrix[Double] = _
  private var _1X: DenseMatrix[Double] = _

  private var _eigen: eig.DenseEig = _


}
