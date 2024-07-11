package com.scalaSparkSkillValidation

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import BankingAppImplicits._

object BankingApp {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val spark: SparkSession = SparkSession.builder().appName("BankApp").master("local[*]").getOrCreate()

    spark.processBankingData()
    spark.stop()
  }
}
