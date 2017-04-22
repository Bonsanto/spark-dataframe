package org.bonsanto

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.Logging

trait SparkApp extends Logging {
  def defaultMasterUrl = "local"

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  lazy val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Simple DataFrame")

  lazy val sc: SparkContext = {
    logDebug("Spark conf: " + conf.toDebugString)
    new SparkContext(conf)
  }

  lazy val sqlContext = new SQLContext(sc)
}