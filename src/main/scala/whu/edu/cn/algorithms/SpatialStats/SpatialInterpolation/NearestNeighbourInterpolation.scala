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
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils
import whu.edu.cn.entity

import scala.math.{max, min}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.interpolationUtils._
import whu.edu.cn.entity.SpaceTimeBandKey

object NearestNeighbourInterpolation {

  def fit(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyName: String, rows: Int = 20, cols: Int = 20,
          method: String = "Sph", binMaxCount: Int = 20):Unit
  //(RDD[(SpaceTimeBandKey, MultibandTile)],TileLayerMetadata[SpaceTimeKey])
  = {

  }

}
