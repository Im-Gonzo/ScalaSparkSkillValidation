package com.scalaSparkSkillValidation.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.scalaSparkSkillValidation.models._

import java.sql.{Timestamp, _}

object DataGenerator {

  def generateSampleData(spark: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    import spark.implicits._

    val accounts = Seq(
      Account("ACC001", Timestamp.valueOf("2024-09-07 09:34:00"), Date.valueOf("2024-09-07"), 5000.00),
      Account("ACC002", Timestamp.valueOf("2024-09-08 02:24:00"), Date.valueOf("2024-09-08"), 2000.00),
      Account("ACC003", Timestamp.valueOf("2024-09-09 11:10:00"), Date.valueOf("2024-09-09"), 9000.00),
      Account("ACC004", Timestamp.valueOf("2024-09-10 05:40:00"), Date.valueOf("2024-09-10"), 5000.00)
    ).toDF()

    val transactions = Seq(
      Transaction("TXN001", Timestamp.valueOf("2024-09-01 11:15:00"), Date.valueOf("2024-09-01"), "ACC001", 1000.00, "Credit"),
      Transaction("TXN002", Timestamp.valueOf("2024-09-01 13:00:00"), Date.valueOf("2024-09-01"), "ACC001", 200.00, "Debit"),
      Transaction("TXN003", Timestamp.valueOf("2024-08-15 03:40:00"), Date.valueOf("2024-08-15"), "ACC003", 2400.00, "Credit"),
      Transaction("TXN004", Timestamp.valueOf("2024-09-09 12:10:00"), Date.valueOf("2024-09-09"), "ACC004", 1400.00, "Debit"),
    ).toDF()

    val customers = Seq(
      Customer("CUST001", Timestamp.valueOf("2024-01-01 09:00:00"), Date.valueOf("2024-01-01"), "John Doe", 30),
      Customer("CUST002", Timestamp.valueOf("2024-01-01 10:00:00"), Date.valueOf("2024-01-01"), "Jane Doe", 28),
      Customer("CUST003", Timestamp.valueOf("2024-02-01 09:00:00"), Date.valueOf("2024-02-01"), "Gonzalo Doe", 30),
      Customer("CUST004", Timestamp.valueOf("2024-02-01 14:00:00"), Date.valueOf("2024-02-01"), "Jean D'Arc", 28)
    ).toDF()

    val accountHolders = Seq(
      AccountHolder("CUST001", "ACC001", "Primary", Timestamp.valueOf("2024-01-01 09:00:00"), Date.valueOf("2024-01-01")),
      AccountHolder("CUST002", "ACC002", "Secondary", Timestamp.valueOf("2024-01-01 10:00:00"), Date.valueOf("2024-01-01")),
      AccountHolder("CUST003", "ACC003", "VIP", Timestamp.valueOf("2024-02-01 09:00:00"), Date.valueOf("2024-02-01")),
      AccountHolder("CUST004", "ACC004", "VIP", Timestamp.valueOf("2024-02-01 14:00:00"), Date.valueOf("2024-02-01"))
    ).toDF()

    (accounts, transactions, customers, accountHolders)
  }
}
