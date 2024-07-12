package com.scalaSparkSkillValidation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j._
import BankingAppSQLImplicits._
import BankingAppDataFrameImplicits._

object BankingApp {

  def processBankingDataSQL(implicit spark: SparkSession): Unit = {
    spark.processBankingDataSQL()
  }

  def processBankingDataDataFrame(implicit spark: SparkSession): Unit = {
    val customerOverallView: DataFrame = spark.createCustomerOverallDataFrame()
    customerOverallView.createOrReplaceTempView("customer_overall_summary_df")
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val spark: SparkSession = SparkSession.builder().appName("BankApp").master("local[*]").getOrCreate()

    processBankingDataSQL
    processBankingDataDataFrame

    spark.stop()
  }
}
