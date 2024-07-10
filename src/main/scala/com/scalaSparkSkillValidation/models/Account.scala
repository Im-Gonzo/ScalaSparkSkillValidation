package com.scalaSparkSkillValidation.models
import java.sql._


/**
 * Representational model of an account
 *
 * @param AccountID: Unique identifier for the account.
 * @param OpenTimestamp: Timestamp when the account was opened.
 * @param OpenDate: Date when account was opened.
 * @param Balance: Current account balance.
 * */
case class Account(AccountID: String,
                   OpenTimestamp: Timestamp,
                   OpenDate: Date,
                   Balance: Double)
