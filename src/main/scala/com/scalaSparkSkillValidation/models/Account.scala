package com.scalaSparkSkillValidation.models
import java.sql._

case class Account(AccountId: String,
                   OpenTimestamp: Timestamp,
                   OpenDate: Date,
                   Balance: Double)
