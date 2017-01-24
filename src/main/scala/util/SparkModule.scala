package util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }

object SparkModule {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("sistem-disposisi-otomatis")
    .set("spark.logConf", "true")
    .set("spark.driver.host", "localhost")
    .set("spark.driver.port", "8080")

  val sparkContext = SparkContext.getOrCreate(sparkConf)

  val sparkSession = SparkSession.builder.getOrCreate()

}