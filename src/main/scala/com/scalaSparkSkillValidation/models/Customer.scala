package com.scalaSparkSkillValidation.models
import java.sql._

case class Customer(CustomerID: String,
                    SignupTimestamp: Timestamp,
                    SignupDate: Date,
                    Name: String,
                    Age: Long)
