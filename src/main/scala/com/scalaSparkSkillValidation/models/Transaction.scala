package com.scalaSparkSkillValidation.models
import java.sql._

/**
 * Representational model of a transaction
 *
 * @param TransactionID: Unique identifier for the transaction.
 * @param TransactionTimestamp: Timestamp when the transaction was made.
 * @param TransactionDate: Date when the transaction was made.
 * @param AccountID: Unique identifier for the account.
 * @param Type: Unique identifier for the type of transaction.
 * */
case class Transaction(TransactionID: String,
                       TransactionTimestamp: Timestamp,
                       TransactionDate: Date,
                       AccountID: String,
                       Amount: Double,
                       Type: String)
