package com.scalaSparkSkillValidation

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.scalaSparkSkillValidation.utils.DataGenerator
import com.scalaSparkSkillValidation.BankingAppDataFrameImplicits._
import org.scalatest.BeforeAndAfterAll

class BankingAppDataFrameTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

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
  }

  "BankingAppDataFrame" should "create joined account view correctly" in {
    val joinedAccountView = spark.createJoinedAccountDataFrame()

    joinedAccountView.columns should contain allOf ("AccountID", "CreatedAt", "CreatedDate", "Balance", "CustomerID", "HolderType", "CreatedAtLocal")
    joinedAccountView.count() shouldBe spark.table("accounts").count()
  }
}
