package com.scalaSparkSkillValidation.models
import java.sql._

case class Account(AccountID: String,
                   OpenTimestamp: Timestamp,
                   OpenDate: Date,
                   Balance: Double)
