package com.scalaSparkSkillValidation


import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.scalaSparkSkillValidation.utils.DataGenerator
import org.scalatest.BeforeAndAfterAll

/**
 * Test suite for BankingApp functionalities.
 * */
class BankingAppSQLTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll  {

  implicit var spark: SparkSession = _

  /**
   * Create spark session for test suite
   * */
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    setupTestData
  }

  /**
   * Tears down the spark session
   * */
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }


  /**
   * Set up common test data
   *
   * @param spark: SparkSession
   * @return Unit: Doesn't return anything
   * */
  def setupTestData(implicit spark: SparkSession): Unit = {
    val (accounts, transactions, customers, accountHolders) = DataGenerator.generateSampleData(spark)
    Seq(
      (accounts, "accounts"),
      (transactions, "transactions"),
      (customers, "customers"),
      (accountHolders, "accountHolders")
    ).foreach { case (df, name) => df.createOrReplaceTempView(name) }
    BankingApp.processBankingDataSQL
  }

  /**
   * Test if BakingApp creates `joined_accounts` temporary view with the correct amount of rows.
   * */
  "BankingApp" should "create joined_accounts view correctly" in {

    /**@note This test assumes `joined_accounts` exists on Spark SQL Catalog*/
    val joinedAccountResult = spark.sql("SELECT * FROM joined_accounts")
    val accounts = spark.table("accounts")
    joinedAccountResult.count() shouldBe accounts.count()

  }

  /**
   * Test if BankingApp create `customer_info` temporary view with the correct columns.
   * */
  it should "create customer_info view with required columns" in {

    /**@note This test assumes `customer_info` exists on Spark SQL Catalog*/
    val customerInfoViewResult = spark.sql("SELECT * FROM customer_info")

    customerInfoViewResult.columns should contain allOf ("CustomerID", "Name", "Age", "AgeGroup")

  }

  /**
   * Test if BankingApp create `transaction_summary` temporary view with the correct information
   * */
  it should "create transaction_summary view correctly" in {

    /**@note This test assumes `transaction_summary` exists on Spark SQL Catalog*/
    val transactionSummaryViewResult = spark.sql("SELECT * FROM transaction_summary")

    transactionSummaryViewResult.columns should contain allOf ("AccountID", "TotalCredits", "TotalDebits", "TransactionCount")

  }

  /**
   * Test if BankingApp create `customer_overall_summary temporary view with the correct information`
   * */
  it should "create customer_overall_summary view correctly" in {

    /**@note This test assumes customer_overall_summary exists on Spark SQL Catalog*/
    val customerSummaryOverviewView = spark.sql("Select * FROM customer_overall_summary")

    customerSummaryOverviewView.columns should contain allOf ("AccountID", "CreatedAtLocal", "FormattedCreatedDate",
                                                              "Balance", "CustomerName", "AgeGroup", "HolderType", "TotalCredits",
                                                              "TotalDebits", "TransactionCount", "CalculatedBalance", "BalanceCheck")

  }
}
