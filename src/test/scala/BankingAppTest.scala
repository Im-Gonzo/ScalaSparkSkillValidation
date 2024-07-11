package com.scalaSparkSkillValidation


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.holdenkarau.spark.testing.SharedSparkContext
import com.scalaSparkSkillValidation.utils.DataGenerator
import org.apache.spark.sql.functions._

/**
 * Test suite for BankingApp functionalities.
 * */
class BankingAppTest extends AnyFlatSpec with Matchers with SharedSparkContext {

  /**
   * Set up common test data
   *
   * @param spark: SparkSession
   * @return Unit: Doesn't return anything
   * */
  def setupTestData(spark: SparkSession): Unit = {
    val (accounts, transactions, customers, accountHolders) = DataGenerator.generateSampleData(spark)
    Seq(
      (accounts, "accounts"),
      (transactions, "transactions"),
      (customers, "customers"),
      (accountHolders, "accountHolders")
    ).foreach { case (df, name) => df.createOrReplaceTempView(name) }
    BankingApp.processBankingData(spark)
  }

  /**
   * Test if BakingApp creates `joined_accounts` temporary view with the correct amount of rows.
   * */
  "BankingApp" should "create joined_accounts view correctly" in {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    setupTestData(spark)

    val joinedAccountResult = spark.sql("SELECT * FROM joined_accounts")
    val accounts = spark.table("accounts")
    joinedAccountResult.count() shouldBe accounts.count()

    spark.stop()
  }

  /**
   * Test if BankingApp create `customer_info` temporary view with the correct columns.
   * */
  it should "create customer_info view with required columns" in {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    setupTestData(spark)

    val customerInfoViewResult = spark.sql("SELECT * FROM customer_info")
    val requiredColumns = Seq("CustomerID", "Name", "Age", "AgeGroup")

    requiredColumns.foreach { col =>
      customerInfoViewResult.columns should contain (col)
    }

    spark.stop()
  }
}
