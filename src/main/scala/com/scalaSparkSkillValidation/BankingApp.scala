package com.scalaSparkSkillValidation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j._
import org.apache.spark.sql.functions._
import com.scalaSparkSkillValidation.models._
import com.scalaSparkSkillValidation.utils.DataGenerator

object BankingApp {

  /**
   * */
  def createJoinedAccountsView(spark: SparkSession): DataFrame ={
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY VIEW joined_accounts AS
        |SELECT
        | a.AccountID,
        | a.CreatedAt,
        | a.CreatedDate,
        | a.Balance,
        | ah.CustomerID,
        | ah.HolderType,
        | from_utc_timestamp(a.CreatedAt, 'UTC') AS CreatedAtLocal
        | FROM accounts a
        | LEFT JOIN accountHolders ah ON a.AccountID = ah.AccountID
        |""".stripMargin)
  }

  /***/
  def executeQueries(spark: SparkSession): DataFrame ={
    createJoinedAccountsView(spark)
  }

  def processBankingData(spark: SparkSession): DataFrame = {
      try{
        executeQueries(spark)
      }catch {
        case e: Exception =>
          println(s"Error processing banking data: ${e.getMessage}")
          spark.emptyDataFrame
      }
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().appName("BankApp").master("local[*]").getOrCreate()

    processBankingData(spark)
    val result = processBankingData(spark)
    result.show(truncate = false)
    spark.stop()
  }
}
