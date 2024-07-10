package com.scalaSparkSkillValidation.models
import java.sql._

/**
 * Representational model of a customer
 *
 * @param CustomerID: Unique identifier for the customer.
 * @param SignupTimestamp: Timestamp when the customer signed in system.
 * @param SignupDate: Date when the customer signed in system.
 * @param Name: Name of the customer.
 * @param Age: Age of the customer.
 * */
case class Customer(CustomerID: String,
                    SignupTimestamp: Timestamp,
                    SignupDate: Date,
                    Name: String,
                    Age: Long)
