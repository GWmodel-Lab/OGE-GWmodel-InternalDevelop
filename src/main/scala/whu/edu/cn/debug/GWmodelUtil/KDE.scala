package whu.edu.cn.debug.GWmodelUtil

import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.spark._
import geotrellis.vector._
import scala.util._
import geotrellis.raster.density.KernelStamper
import geotrellis.raster.mapalgebra.local.LocalTileBinaryOp
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster._
import geotrellis.spark.tiling._
import geotrellis.spark._


object KDE {
  //主函数
  def main(args: Array[String]):Unit= {
    val extent=Extent(-109,37,-102,41)

    def randomPointFeature(extent: Extent):PointFeature[Double]={
      def randInRange(low:Double,high:Double):Double={
        val x=Random.nextDouble
        low*(1-x)+high*x
      }
      Feature(Point(randInRange(extent.xmin,extent.xmax),
        randInRange(extent.ymin,extent.ymax)),
        Random.nextInt%16+16)
    }
    val pts=(for(i <- 1 to 1000) yield randomPointFeature(extent)).toList
    println(pts)

    val kernelWidth:Int=999
    val kern:Kernel=Kernel.gaussian(kernelWidth,15,25)
    val kde:Tile=pts.kernelDensity(kern,RasterExtent(extent,700,400))

    val colorMap=ColorMap(
      (0 to kde.findMinMax._2 by 4).toArray,
      ColorRamps.HeatmapBlueToYellowToRedSpectrum
    )
    kde.renderPng(colorMap).write("test.png")
  }

}
