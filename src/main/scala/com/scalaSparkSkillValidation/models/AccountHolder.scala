package com.scalaSparkSkillValidation.models

import java.sql._

case class AccountHolder(CustomerID: String, AccountID: String, Relationship: String, AddedTimestamp: Timestamp, AddedDate: Date)
