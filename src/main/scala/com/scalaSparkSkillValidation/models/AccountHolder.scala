package com.scalaSparkSkillValidation.models

import java.sql._

/**
 * Representational model of an account holder
 *
 * @param CustomerID: Unique identifier for the customer.
 * @param AccountID: Unique identifier for the account.
 * @param Relationship: Relationship with the client.
 * @param AddedTimestamp: Timestamp when the account was added.
 * @param AddedDate: Date when the account was added.
 * */
case class AccountHolder(CustomerID: String,
                         AccountID: String,
                         Relationship: String,
                         AddedTimestamp: Timestamp,
                         AddedDate: Date)
