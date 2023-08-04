package whu.edu.cn.debug.GWmodelUtil

import scala.math._

object optimize {

//  def goldenSelection(lower:Double, upper:Double, eps:Double = 1e-8, f:SARlagmodel)={
//    var iter: Int = 0
//    val max_iter=100
//    val ratio:Double = (sqrt(5) - 1) / 2;
//
//    var a = lower
//    var b = upper
//    var step = b - a
//    var p = a + (1 - ratio) * step
//    var q = a + ratio * step
//    var f_a:Double=f.func4optimize(a)
//    var f_b:Double=f.func4optimize(b)
//    var f_p:Double=f.func4optimize(p)
//    var f_q:Double=f.func4optimize(q)
////    var status=Array[Double](4)
////    while ( abs(f_a - f_b) >= eps && iter < max_iter) {
////      if (f_p > f_q) {
////        b = q;
////        f_b = f_q;
////        q = p;
////        f_q = f_p;
////        step = b - a;
////        p = a + (1 - ratio) * step;
////        status(2) = u64(RsquareByLambda(bw, p, f_p));
////      }
////      else {
////        a = p;
////        f_a = f_p;
////        p = q;
////        f_p = f_q;
////        step = b - a;
////        q = a + ratio * step;
////        status(3) = u64(RsquareByLambda(bw, q, f_q));
////      }
////      iter +=1
////    }
//  }

}
