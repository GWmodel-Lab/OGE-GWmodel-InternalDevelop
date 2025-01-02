package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{DoubleCellType, MultibandTile, RasterExtent, Tile, TileLayout}
import geotrellis.raster.interpolation.{OrdinaryKrigingMethods, SimpleKrigingMethods}
import geotrellis.spark.withFeatureRDDRasterizeMethods
import geotrellis.vector
import geotrellis.vector.interpolation.{EmpiricalVariogram, Exponential, Gaussian, Kriging, NonLinearSemivariogram, OrdinaryKriging, Semivariogram, Spherical}
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
   *
   * @param featureRDD
   * @param propertyName
   * @param modelType
   * @return
   */
  //块金：nugget，基台：sill，变程：range
  def OrdinaryKriging(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyName: String, rows: Int = 20, cols: Int = 20,
                      method : String = "Sph", binMaxCount: Int = 20) = {
    val extents = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)
//    //slow code
//    val rasterExtent = RasterExtent(extent, cols, rows)
//    val pointsRas = (for {
//      row <- 0 until rows
//      col <- 0 until cols
//    } yield {
//      val (x, y) = rasterExtent.gridToMap(col, row)
//      vector.Point(x, y)
//    }).toArray
    val cellWidth = (extent.xmax - extent.xmin) / cols
    val cellHeight = (extent.ymax - extent.ymin) / rows
    val pointsRas = (for {
      row <- 0 until rows
      col <- 0 until cols
    } yield {
      val x = extent.xmin + col * cellWidth
      val y = extent.ymax - (row + 1) * cellHeight // 反转y轴
      vector.Point(x, y)
    }).toArray

    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      val data = t._2._2(propertyName).asInstanceOf[String].toDouble
      PointFeature(p, data)
    }).collect()
    //    println(points.toList)
    val coors = featureRDD.map(t => t._2._1.getCentroid.getCoordinate).collect()
    val distances = for {
      i <- coors.indices
      j <- i + 1 until coors.length
    } yield coors(i).distance(coors(j))
    val averageDistance = distances.sum / distances.size
    val es: EmpiricalVariogram = EmpiricalVariogram.nonlinear(points, 2 * averageDistance, binMaxCount)
    //    println(s"EmpiricalVariogram distances: ${es.distances.mkString(", ")}")
    //    println(s"EmpiricalVariogram variances: ${es.variance.mkString(", ")}")
    val gamma = es.distances.length
    val pRange = es.distances(gamma / 2)
    val pSill = es.variance(gamma / 2)
    val pNugget = es.variance(0)

    val sv = method match {
      case "Sph" => NonLinearSemivariogram(range = pRange, sill = pSill, nugget = pNugget, Spherical)
      case "Gau" => NonLinearSemivariogram(range = pRange, sill = pSill, nugget = pNugget, Gaussian)
      case "Exp" => NonLinearSemivariogram(range = pRange, sill = pSill, nugget = pNugget, Exponential)
      case _ => throw new IllegalArgumentException(s"Unsupported method: $method")
    }
    println("************************************************************")
    println(s"Parameters load correctly, start calculation")
    println(f"Semivariogram function\n  range:$pRange%.3f, psill:$pSill%.3f, nugget:$pNugget%.3f")
    //    //fit error
    //    val svSpherical: Semivariogram = Semivariogram.fit(es, Spherical)
    //    println(s"Fitted Semivariogram: $svSpherical")
    //    val sv = NonLinearSemivariogram(range = effectiveRange, sill = effectiveSill, nugget = effectiveNugget, Spherical)

    val kriging = new OrdinaryKriging(points, averageDistance, sv)
    //    val location = featureRDD.map(t => {vector.Point(t._2._1.getCoordinate)} ).collect()
    //    val predictions = kriging.predict(location)
    //    predictions.foreach(t=>println(t._1))
    val predictions = kriging.predict(pointsRas)
    //    predictions.foreach(println)

    val crs = try {
      geotrellis.proj4.CRS.fromEpsgCode(featureRDD.first()._2._1.getSRID)
    } catch {
      case e: UnknownAuthorityCodeException => geotrellis.proj4.CRS.fromEpsgCode(4326)
    }
    //    println(crs)

    //output
    val tl = TileLayout(1, 1, cols, rows)
    val ld = LayoutDefinition(extent, tl)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    //output raster value
    val featureRaster = pointsRas.zipWithIndex.map(t => {
      val data = predictions(t._2)._1
      val geom = t._1
      val feature = new vector.Feature[Geometry, Double](geom, data)
      feature
    })
    //make rdd
    val featureRDDforRaster = sc.makeRDD(featureRaster)
    val originCoverage = featureRDDforRaster.rasterize(cellType, ld)
    val imageRDD = originCoverage.map(t => {
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("kriging_interpolation"))
      val v = MultibandTile(t._2)
      (k, v)
    })
    println("kriging interpolation succeeded")
    println("************************************************************")
    (imageRDD, tileLayerMetadata)
  }

  def selfDefinedKriging(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyName: String, rows: Int = 20, cols: Int = 20,
                         method: String = "Sph", range: Double = 0, sill: Double = 0, nugget: Double = 0) = {
    val extents = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)
    val cellWidth = (extent.xmax - extent.xmin) / cols
    val cellHeight = (extent.ymax - extent.ymin) / rows
    val pointsRas = (for {
      row <- 0 until rows
      col <- 0 until cols
    } yield {
      val x = extent.xmin + col * cellWidth
      val y = extent.ymax - (row + 1) * cellHeight
      vector.Point(x, y)
    }).toArray
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      val data = t._2._2(propertyName).asInstanceOf[String].toDouble
      PointFeature(p, data)
    }).collect()
    val halfDistance = max((extent.xmax - extent.xmin),(extent.ymax - extent.ymin))/2
    if (range <= 0) throw new IllegalArgumentException("Range must be over 0!")
    if (sill <= 0) throw new IllegalArgumentException("Sill must be over 0!")
    if (nugget <= 0) throw new IllegalArgumentException("Nugget must be over 0!")

    val sv = method match {
      case "Sph" => NonLinearSemivariogram(range = range, sill = sill, nugget = nugget, Spherical)
      case "Gau" => NonLinearSemivariogram(range = range, sill = sill, nugget = nugget, Gaussian)
      case "Exp" => NonLinearSemivariogram(range = range, sill = sill, nugget = nugget, Exponential)
      case _ => throw new IllegalArgumentException(s"Unsupported method: $method")
    }
    println("************************************************************")
    println(f"Parameters load correctly, start calculation")
    println(f"Semivariogram function\n  range:$range%.3f, psill:$sill%.3f, nugget:$nugget%.3f")
    val kriging = new OrdinaryKriging(points, halfDistance, sv)
    val predictions = kriging.predict(pointsRas)

    val crs = try {
      geotrellis.proj4.CRS.fromEpsgCode(featureRDD.first()._2._1.getSRID)
    } catch {
      case e: UnknownAuthorityCodeException => geotrellis.proj4.CRS.fromEpsgCode(4326)
    }

    //output
    val tl = TileLayout(1, 1, cols, rows)
    val ld = LayoutDefinition(extent, tl)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    val cellType = DoubleCellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)
    val featureRaster = pointsRas.zipWithIndex.map(t => {
      val data = predictions(t._2)._1
      val geom = t._1
      val feature = new vector.Feature[Geometry, Double](geom, data)
      feature
    })
    val featureRDDforRaster = sc.makeRDD(featureRaster)
    val originCoverage = featureRDDforRaster.rasterize(cellType, ld)
    val imageRDD = originCoverage.map(t => {
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("kriging_interpolation"))
      val v = MultibandTile(t._2)
      (k, v)
    })
    println("kriging interpolation succeeded")
    println("************************************************************")
    (imageRDD, tileLayerMetadata)
  }

}
