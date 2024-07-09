package com.scalaSparkSkillValidation.models
import java.sql._

case class Transaction(TransactionID: String,
                       TransactionTimestamp: Timestamp,
                       TransactionDate: Date,
                       AccountID: String,
                       Amount: Double,
                       Type: String)
