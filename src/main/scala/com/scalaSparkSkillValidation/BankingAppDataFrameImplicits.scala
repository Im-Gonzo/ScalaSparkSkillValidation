package com.scalaSparkSkillValidation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BankingAppDataFrameImplicits {

  implicit class BankingAppDataFrameExtension(spark: SparkSession)
  {

    /**
     * Creates a joined DataFrame of accounts and accountHolders
     *
     * @return DataFrame: The joined and transformed data
     */
    def createJoinedAccountDataFrame(): DataFrame = {
      val accounts = spark.table("accounts")
      val accountHolders = spark.table("accountHolders")

      accounts.join(accountHolders, Seq("AccountID"), "left")
        .select(
          col("AccountID"),
          col("OpenTimestamp").as("CreatedAt"),
          col("OpenDate").as("CreatedDate"),
          col("Balance"),
          col("CustomerID"),
          col("Relationship").as("HolderType"),
          from_utc_timestamp(col("OpenTimestamp"), "UTC").as("CreatedAtLocal")
        )
    }

    /**
     * Creates a DataFrame formated with customer information
     *
     * @return DataFrame: The formatted data
     * */
    def createCustomerInfoDataFrame(): DataFrame = {
      val customers = spark.table("customers")

      customers.select(
        col("CustomerID"),
        col("Name"),
        col("Age"),
        when(col("Age") < 30, "Young")
          .when(col("Age").between(30, 59), "Middle")
          .otherwise("Senior").as("AgeGroup")
      )
    }

    /**
     * Creates a joined DataFrame of accounts and transactions
     *
     * @return DataFrame: The joined and transformed data
     * */
    def createTransactionSummaryDataFrame(): DataFrame = {
      val accounts = spark.table("accounts")
      val transactions = spark.table("transactions")

      accounts.join(transactions, Seq("AccountID"), "left")
        .groupBy("AccountID")
        .agg(
          coalesce(sum(when(col("Type") === "Credit", col("Amount")).otherwise(0)), lit(0).as("TotalCredits")),
          coalesce(sum(when(col("Type") === "Debit", col("Amount")).otherwise(0)), lit(0).as("TotalDebits")),
          count("TransactionID").as("TransactionCount")
        )
    }

    /**
     * Creates a joined DataFrame of joined_accounts, customer_info, transaction_summary
     *
     * @return DataFrame: The joined and transformed data
     * */
    def createCustomerOverallDataFrame(): DataFrame = {
      val joinedAccounts = createJoinedAccountDataFrame()
      val customerInfo = createCustomerInfoDataFrame()
      val transactionSummary = createTransactionSummaryDataFrame()

      joinedAccounts.join(customerInfo, Seq("CustomerID"))
        .join(transactionSummary, Seq("AccountID"), "left")
        .select(
          col("AccountID"),
          col("CreatedAtLocal"),
          date_format(col("CreatedDate"), "yyyy-MM-dd").as("FormattedCreatedDate"),
          col("Balance"),
          col("Name").as("CustomerName"),
          col("AgeGroup"),
          col("HolderType"),
          coalesce(col("TotalCredits"), lit(0).as("TotalCredits")),
          coalesce(col("TotalDebits"), lit(0).as("TotalDebits")),
          coalesce(col("TransactionCount"), lit(0).as("TransactionCount")),
          (col("Balance") + coalesce(col("TotalCredits"), lit(0)) - coalesce(col("TotalDebits"), lit(0))).as("CalculatedBalance")
        )
        .withColumn("BalanceCheck",
          when(col("CalculatedBalance") =!= col("Balance"), "Mismatch")
            .otherwise("Match")
        )
        .orderBy("AccountID")

    }
  }

}
