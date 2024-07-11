package com.scalaSparkSkillValidation


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.holdenkarau.spark.testing.SharedSparkContext
import com.scalaSparkSkillValidation.utils.DataGenerator
import org.apache.spark.sql.functions._


class BankingAppTest extends AnyFlatSpec with Matchers with SharedSparkContext {

  "BankingApp" should "process banking data correctly" in {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    import spark.implicits._

    // Load dummy data
    val (accounts, transactions, customers, accountHolders) = DataGenerator.generateSampleData(spark)

    // Register DataFrames in Spark SQL Catalog.
    accounts.createOrReplaceTempView("accounts")
    transactions.createOrReplaceTempView("transactions")
    customers.createOrReplaceTempView("customers")
    accountHolders.createOrReplaceTempView("accountHolders")

    BankingApp.processBankingData(spark)

    /**@note Data for testing gets pulled from Spark SQL Catalog*/
    val joinedAccountResult = spark.sql("SELECT * FROM joined_accounts")

    joinedAccountResult.count() shouldBe accounts.count()


  }
}
