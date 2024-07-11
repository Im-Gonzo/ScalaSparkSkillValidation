package com.scalaSparkSkillValidation

import org.apache.spark.sql.SparkSession
import org.apache.log4j._


object BankingApp {

  /**
   * Creates a temporary view that joins accounts and accountHolders tables
   *
   * @param spark: SparkSession
   * @return Unit: Doesn't return anything
   *
   * @note This function assumes `accounts` and `accountHolders` exists in the Spark SQL Catalog.
   * */
  def createJoinedAccountsView(spark: SparkSession): Unit ={
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY VIEW joined_accounts AS
        |SELECT
        | a.AccountID,
        | a.OpenTimestamp as CreatedAt,
        | a.OpenDate as CreatedDate,
        | a.Balance,
        | ah.CustomerID,
        | ah.Relationship as HolderType,
        | from_utc_timestamp(a.OpenTimestamp, 'UTC') AS CreatedAtLocal
        | FROM accounts a
        | LEFT JOIN accountHolders ah ON a.AccountID = ah.AccountID
        |""".stripMargin)
  }

  /**
   * Creates a temporary view that shows customer information
   *
   * @params spark: SparkSession
   * @return Unit: Doesn't return anything
   * @note This function assumes `customers` exists in the Spark SQL Catalog.
   * */
  def createCustomerInfoView(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY VIEW customer_info AS
        |SELECT
        | c.CustomerID,
        | c.Name,
        | c.Age,
        | CASE
        |   WHEN c.Age < 30 THEN 'Young'
        |   WHEN c.Age >= 30 AND c.Age < 60 THEN 'Middle-aged'
        |   WHEN c.Age > 60 THEN 'Senior'
        |  END AS AgeGroup
        | FROM customers AS c
        |""".stripMargin)
  }


  /**
   * Creates a temporary view that shows transactions from customers formatted.
   *
   * @param spark: SparkSession
   * @return Unit: Doesn't return anything
   * @note This function assumes `transactions` exists in the Spark SQL Catalog
   * */
  def createTransactionSummaryView(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE OR REPLACE TEMPORARY VIEW transaction_summary AS
        |SELECT
        | a.AccountID,
        | COALESCE(SUM(CASE WHEN t.Type = 'Credit' THEN t.Amount ELSE 0 END)) AS TotalCredits,
        | COALESCE(SUM(CASE WHEN t.Type = 'Debit' THEN t.Amount ELSE 0 END)) AS TotalDebits,
        | COUNT(t.TransactionID) AS TransactionCount
        |FROM accounts a
        |LEFT JOIN transactions t ON a.AccountID = t.AccountID
        |GROUP BY a.AccountID
        |""".stripMargin)
  }

  /***/
  def createCustomerOverallView(spark: SparkSession): Unit = {
    spark.sql(
      """
        | CREATE OR REPLACE TEMPORARY VIEW customer_overall_summary AS
        | SELECT
        |   ja.AccountID,
        |   ja.CreatedAtLocal,
        |   date_format(ja.CreatedDate, 'yyyy-MM-dd') AS FormattedCreatedDate,
        |   ja.Balance,
        |   ci.Name AS CustomerName,
        |   ci.AgeGroup,
        |   ja.HolderType,
        |   COALESCE(ts.TotalCredits, 0) AS TotalCredits,
        |   COALESCE(ts.TotalDebits, 0) AS TotalDebits,
        |   COALESCE(ts.TransactionCount, 0) AS TransactionCount,
        |   (ja.Balance + COALESCE(ts.TotalCredits, 0) - COALESCE(ts.TotalDebits, 0)) AS CalculatedBalance,
        |   CASE
        |     WHEN (ja.Balance + COALESCE(ts.TotalCredits, 0) - COALESCE(ts.TotalDebits, 0)) <> ja.Balance THEN 'Mismatch'
        |     ELSE 'Match'
        |   END AS BalanceCheck
        | FROM joined_accounts ja
        | JOIN customer_info ci ON ja.CustomerID = ci.CustomerID
        | LEFT JOIN transaction_summary ts ON ja.AccountID = ts.AccountID
        | ORDER BY ja.AccountID
        |""".stripMargin)
  }

  /**
   * Process banking data by creating a series of views available in the Spark SQL Catalog
   *
   * @param spark: SparkSession
   * @throws Exception: Handles exceptions internally.
   *
   * @note This function assumes that `accounts`, `transactions`, `customers`, `accountHolders`
   *       tables/views exists in the Spark SQL Catalog.
   * */
  def processBankingData(spark: SparkSession): Unit = {
      try{
        createJoinedAccountsView(spark)
        createCustomerInfoView(spark)
        createTransactionSummaryView(spark)
        createCustomerOverallView(spark)
      }catch {
        case e: Exception =>
          println(s"Error processing banking data: ${e.getMessage}")
      }
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().appName("BankApp").master("local[*]").getOrCreate()

    processBankingData(spark)
    spark.stop()
  }
}
