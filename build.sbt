name := "mesa"

version := "1.2.0-SNAPSHOT"

organization := "xyz.callide"

scalaVersion := "2.12.6"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7" % Test

// https://mvnrepository.com/artifact/com.univocity/univocity-parsers
libraryDependencies += "com.univocity" % "univocity-parsers" % "2.8.2"

// https://mvnrepository.com/artifact/org.apache.poi/poi
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "4.1.1"
