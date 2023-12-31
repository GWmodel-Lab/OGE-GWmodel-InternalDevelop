package whu.edu.cn.oge

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.alibaba.fastjson.JSONObject
import whu.edu.cn.config.GlobalConfig
import whu.edu.cn.config.GlobalConfig.DagBootConf.DAG_ROOT_URL
import whu.edu.cn.entity.{CoverageCollectionMetadata, SpaceTimeBandKey}
import whu.edu.cn.geocube.core.entity.RasterTileLayerMetadata
import whu.edu.cn.trigger.Trigger
import whu.edu.cn.util.HttpRequestUtil.sendPost
import whu.edu.cn.util.PostSender

object Service {

  def getCoverageCollection(productName: String, dateTime: String = null, extent: String = null): CoverageCollectionMetadata = {
    val coverageCollectionMetadata: CoverageCollectionMetadata = new CoverageCollectionMetadata()
    coverageCollectionMetadata.setProductName(productName)
    if (extent != null) {
      coverageCollectionMetadata.setExtent(extent)
    }
    if (dateTime != null) {
      val timeArray: Array[String] = dateTime.replace("[", "").replace("]", "").split(",")
      coverageCollectionMetadata.setStartTime(timeArray.head)
      coverageCollectionMetadata.setEndTime(timeArray(1))
    }
    coverageCollectionMetadata
  }

//  def getCoverage(implicit sc: SparkContext, coverageId: String,productKey: String="3", level: Int = 0): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//
//    val time1: Long = System.currentTimeMillis()
//    val coverage = Coverage.load(sc, coverageId, productKey,level)
//    println("Loading data Time: "+ (System.currentTimeMillis()-time1))
//    coverage
//  }

  def getFeature(implicit sc: SparkContext, featureId: String,dataTime:String= null,crs:String="EPSG:4326")={
    Feature.load(sc,featureId,dataTime = dataTime,crs)
  }


//  def getCube(implicit sc: SparkContext, CubeName: String, extent: String = null, dateTime: String = null): (RDD[(whu.edu.cn.geocube.core.entity.SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
//    Cube.load(sc, cubeName = CubeName, extent = extent, dateTime = dateTime)
//  }

  def print(res:String,name:String,valueType:String):Unit={
//    val j = new JSONObject()
//    j.put("name",name)
//    j.put("value",res)
//    j.put("type",valueType)
//    Trigger.outputInformationList.append(j)
//
//
//    val jsonObject: JSONObject = new JSONObject
//
//    jsonObject.put("info",Trigger.outputInformationList.toArray)
//
//    val outJsonObject: JSONObject = new JSONObject
//    outJsonObject.put("workID", Trigger.dagId)
//    outJsonObject.put("json", jsonObject)
//    println(outJsonObject)
//    PostSender.shelvePost("info",Trigger.outputInformationList.toArray)
    println(name)
    println(res)
  }


}
