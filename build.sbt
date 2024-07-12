ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "ScalaSparkSkillValidation",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.0.0",
      "org.apache.spark" %% "spark-sql" % "3.0.0",
      "org.scalatest" %% "scalatest" % "3.2.9" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "3.0.0_1.1.0" % "test",
      "org.apache.logging.log4j" % "log4j-core" % "2.13.3",
      "org.apache.logging.log4j" % "log4j-api" % "2.13.3",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.13.3"
    )
  )