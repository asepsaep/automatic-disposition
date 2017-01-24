package utils

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }

import net.ceedubs.ficus.Ficus._

object SparkModule {

  val config: Config = ConfigFactory.load()

  val sparkConf = new SparkConf()
    .setMaster(config.as[String]("spark.master"))
    .setAppName(config.as[String]("spark.appName"))
    .set("spark.logConf", config.as[String]("spark.log"))
    .set("spark.driver.host", config.as[String]("spark.host"))
    .set("spark.driver.port", config.as[String]("spark.port"))

  val sparkContext = SparkContext.getOrCreate(sparkConf)

  val sparkSession = SparkSession.builder.getOrCreate()

}