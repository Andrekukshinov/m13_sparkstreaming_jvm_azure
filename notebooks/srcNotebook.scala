// Databricks notebook source
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

// COMMAND ----------

val storageName = spark.conf.get("stor.acc.name")

// COMMAND ----------

val containerName = spark.conf.get("contain.name")

// COMMAND ----------

val basePath = "abfss://" + containerName + "@" + storageName + ".dfs.core.windows.net/m13sparkstreaming/"

// COMMAND ----------

val srcPath = basePath + "hotel-weather/"

// COMMAND ----------

val pathForStream = basePath + "dst/"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Running stream from the blob where the data will be increamantally delivered

// COMMAND ----------

    val hotelsSchema = StructType(Seq())
      .add(StructField("address", StringType, true, Metadata.empty))
      .add(StructField("avg_tmpr_c", DoubleType, true, Metadata.empty))
      .add(StructField("avg_tmpr_f", DoubleType, true, Metadata.empty))
      .add(StructField("country", StringType, true, Metadata.empty))
      .add(StructField("city", StringType, true, Metadata.empty))
      .add(StructField("id", BinaryType, true, Metadata.empty))
      .add(StructField("latitude", DoubleType, true, Metadata.empty))
      .add(StructField("longitude", DoubleType, true, Metadata.empty))
      .add(StructField("geohash", StringType, true, Metadata.empty))
      .add(StructField("name", StringType, true, Metadata.empty))
      .add(StructField("wthr_date", StringType, true, Metadata.empty))
      .add(StructField("wthr_year", StringType, true, Metadata.empty))
      .add(StructField("wthr_month", StringType, true, Metadata.empty))
      .add(StructField("wthr_day", StringType, true, Metadata.empty))


// COMMAND ----------

val hotelSream = spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format", "parquet")
.option("mergeSchema", "true")
.option("cloudFiles.schemaLocation", "/dbfs/tmp/checkpoint/")
.schema(hotelsSchema)
.load(pathForStream)

// COMMAND ----------

hotelSream.writeStream.format("delta").option("mergeSchema", "true").option("checkpointLocation", "/dbfs/tmp/checkpoint/").table("sreaming_hotel")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Incremental copy from scr to destination

// COMMAND ----------

  def updatePathTillDay(path: String, paths: ListBuffer[String]): Unit = {
    val lsed = dbutils.fs.ls(path)
    lsed.foreach(fsUnit => {
      if (fsUnit.isDir && !fsUnit.path.contains("day")) {
        updatePathTillDay(fsUnit.path, paths)
      } else if (fsUnit.isDir){
        paths += fsUnit.path.stripPrefix(srcPath)
      }
    })
  }

// COMMAND ----------

val relativePaths: ListBuffer[String] = new ListBuffer[String]()
updatePathTillDay(srcPath, relativePaths)

// COMMAND ----------

def loadFilesFromToWithinInterval(relativePaths: ListBuffer[String], hotelPath:String, dst: String, waitInSeconds: Int): Unit = {
  relativePaths.foreach(relativePath => {
    dbutils.fs.cp(hotelPath + relativePath, dst + relativePath, recurse=true)
    println("File " + hotelPath + relativePath + " is copied")
    try {
      Thread.sleep(waitInSeconds * 1000)
    } catch {
      case ex: InterruptedException =>
        throw new RuntimeException("therad was interrupted")
    }
  })
}

// COMMAND ----------

loadFilesFromToWithinInterval(relativePaths, srcPath, pathForStream, 1)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Get cities with max amount of weather reports from hotels 

// COMMAND ----------

val countedHotelsPerCityDay = spark.sql("SELECT DISTINCT CITY, wthr_date, COUNT(*) AS HOTELS_PER_CITY FROM sreaming_hotel GROUP BY CITY, wthr_date ORDER BY HOTELS_PER_CITY DESC")

// COMMAND ----------

countedHotelsPerCityDay.write.format("delta").option("mergeSchema", "true").saveAsTable("hotles_per_city_day")

// COMMAND ----------

val top10CityiesByHotelAmount = spark.sql("SELECT DISTINCT CITY, MAX(HOTELS_PER_CITY) AS MAX_HOTELS_PER_CITY FROM hotles_per_city_day GROUP BY CITY ORDER BY MAX_HOTELS_PER_CITY desc LIMIT 10")

// COMMAND ----------

top10CityiesByHotelAmount.write.format("delta").option("mergeSchema", "true").saveAsTable("ten_biggest_cities")

// COMMAND ----------

spark.sql("SELECT CITY, MAX_HOTELS_PER_CITY, ROW_NUMBER() OVER (ORDER BY MAX_HOTELS_PER_CITY DESC) AS number FROM ten_biggest_cities").write.format("delta").option("mergeSchema", "true").saveAsTable("ten_biggest_cities_ordered")

// COMMAND ----------

spark.sql("SELECT CITY, MAX_HOTELS_PER_CITY, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS number FROM ten_biggest_cities ORDER BY MAX_HOTELS_PER_CITY desc").explain(mode="formatted")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Prepearing result containing top 10: max, avg, min tmp in celcium for each city where the max amount of hotel reports was found during tracking period

// COMMAND ----------

val temperatureValuesForCities = spark.sql("SELECT DISTINCT CITY, wthr_date, AVG(avg_tmpr_c) as avg_temperature, MAX(avg_tmpr_c) as max_temperature, MIN(avg_tmpr_c) as min_temperature FROM sreaming_hotel GROUP BY CITY, wthr_date")

// COMMAND ----------

temperatureValuesForCities.write.format("delta").option("mergeSchema", "true").saveAsTable("temerature_hotel")

// COMMAND ----------

val result = spark.sql("SELECT th.avg_temperature, th.max_temperature, th.min_temperature, tbco.MAX_HOTELS_PER_CITY, th.CITY, th.wthr_date, tbco.number, COUNT(sh.id) as hotel_per_city_date FROM temerature_hotel th INNER JOIN ten_biggest_cities_ordered tbco ON tbco.CITY = th.CITY INNER JOIN sreaming_hotel sh ON sh.CITY = th.CITY AND sh.wthr_date = th.wthr_date GROUP BY th.avg_temperature, th.max_temperature, th.min_temperature, tbco.MAX_HOTELS_PER_CITY, th.CITY, th.wthr_date, tbco.number ORDER BY tbco.MAX_HOTELS_PER_CITY DESC")


// COMMAND ----------

display(result)

// COMMAND ----------

result.persist(StorageLevel.MEMORY_ONLY)

// COMMAND ----------

result.explain(mode="formatted")

// COMMAND ----------

def displayCityFromListOfTop10Cities(cityNumber: Int): Unit = {
  display(result.select("*").where(result.col("number") === lit(cityNumber)))
}

// COMMAND ----------

displayCityFromListOfTop10Cities(1)

// COMMAND ----------

displayCityFromListOfTop10Cities(2)

// COMMAND ----------

displayCityFromListOfTop10Cities(3)

// COMMAND ----------

displayCityFromListOfTop10Cities(4)

// COMMAND ----------

displayCityFromListOfTop10Cities(5)

// COMMAND ----------

displayCityFromListOfTop10Cities(6)

// COMMAND ----------

displayCityFromListOfTop10Cities(7)

// COMMAND ----------

displayCityFromListOfTop10Cities(8)

// COMMAND ----------

displayCityFromListOfTop10Cities(9)

// COMMAND ----------

displayCityFromListOfTop10Cities(10)
