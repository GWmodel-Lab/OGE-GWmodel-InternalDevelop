package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, RasterExtent, Tile, TileLayout}
import geotrellis.raster.interpolation.{OrdinaryKrigingMethods, SimpleKrigingMethods}
import geotrellis.vector
import geotrellis.vector.interpolation.{EmpiricalVariogram, Kriging, NonLinearSemivariogram, OrdinaryKriging, Semivariogram, Spherical}
import geotrellis.vector.{Extent, PointFeature}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import org.locationtech.proj4j.UnknownAuthorityCodeException
import whu.edu.cn.entity

import scala.math.{max, min}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import whu.edu.cn.entity.SpaceTimeBandKey

object Kriging {

  /**
   * TODO: fix bug
   * TODO: 简单克里金插值(奇异矩阵报错), modelType参数未使用，是否需要输入掩膜
   *
   * @param featureRDD
   * @param propertyName
   * @param modelType
   * @return
   */
  //块金：nugget，基台：sill，变程：range
  def OrdinaryKriging(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyName: String, rows:Int =256, cols:Int =256) = {
    val extents = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)
    val rasterExtent = RasterExtent(extent, cols, rows)
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      val data = t._2._2(propertyName).asInstanceOf[String].toDouble
      PointFeature(p, data)
    }).collect()
    println(points.toList)

    val location=featureRDD.map(t=>{
      vector.Point(t._2._1.getCoordinate)
    }).collect()

    val maxDistanceBandwidth=10000
    val binMaxCount=1
//    val es: EmpiricalVariogram = EmpiricalVariogram.nonlinear(points, maxDistanceBandwidth, binMaxCount)
//    val svSpherical: Semivariogram = Semivariogram.fit(es, Spherical)


    val sv = NonLinearSemivariogram(maxDistanceBandwidth, binMaxCount, Spherical)
    println(sv.sill,sv.range,sv.nugget)

    val krigingVal: Array[(Double, Double)] =
      new OrdinaryKriging(points, 5000, sv)
        .predict(location)

    val method = new OrdinaryKrigingMethods {
      override def self: Traversable[PointFeature[Double]] = points
    }
    val originCoverage = method.ordinaryKriging(rasterExtent, sv)
    println(originCoverage)

    val tl = TileLayout(1, 1, cols, rows)
    val ld = LayoutDefinition(extent, tl)
    val crs = try {
      geotrellis.proj4.CRS.fromEpsgCode(featureRDD.first()._2._1.getSRID)
    } catch {
      case e:UnknownAuthorityCodeException => geotrellis.proj4.CRS.fromEpsgCode(4326)
    }
    println(crs)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    println(bounds)
    //cellType的定义?
//    val cellType = originCoverage.cellType
//    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
//
//    originCoverage.toArrayTile()
//    var list: List[Tile] = List.empty
//    list = list :+ originCoverage
//    val tileRDD = sc.parallelize(list)
//    val imageRDD = tileRDD.map(t => {
//      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("KrigingInterpolation"))
//      val v = MultibandTile(t)
//      (k, v)
//    })
//
//    (imageRDD, tileLayerMetadata)
  }

//  def makeRasterRDDFromTif(sc: SparkContext, input: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//                           sourceTiffpath: String) = {
//    val hadoopPath = "file://" + sourceTiffpath
//    val layout = input._2.layout
//    val inputRdd = sc.hadoopMultibandGeoTiffRDD(new Path(hadoopPath))
//    val tiled = inputRdd.tileToLayout(input._2.cellType, layout, Bilinear)
//    val srcLayout = input._2.layout
//    val srcExtent = input._2.extent
//    val srcCrs = input._2.crs
//    val cellType = input._2.cellType
//    val srcBounds = input._2.bounds
//    val now = "1000-01-01 00:00:00"
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val date = sdf.parse(now).getTime
//    val newBounds = Bounds(SpaceTimeKey(srcBounds.get.minKey.spatialKey._1, srcBounds.get.minKey.spatialKey._2, date), SpaceTimeKey(srcBounds.get.maxKey.spatialKey._1, srcBounds.get.maxKey.spatialKey._2, date))
//    val metaData = TileLayerMetadata(cellType, srcLayout, srcExtent, srcCrs, newBounds)
//    val tiledOut = tiled.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1._1, t._1._2, date), ListBuffer("Grass")), t._2)
//    })
//    println("成功读取tif")
//    (tiledOut, metaData)
//  }


}
