name := "mesa"

version := "0.1.0-SNAPSHOT"

organization := "xyz.callide"

scalaVersion := "2.12.6"

libraryDependencies += "xyz.callide" %% "common" % "0.1.0-SNAPSHOT"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.11"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7" % Test

// https://mvnrepository.com/artifact/org.apache.commons/commons-csv
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.7"

// https://mvnrepository.com/artifact/com.univocity/univocity-parsers
libraryDependencies += "com.univocity" % "univocity-parsers" % "2.8.2"
