name := "maven-dependency-graph"

version := "1.0"

scalaVersion := "2.10.5"

val sparkCore = "org.apache.spark" % "spark-core_2.10" % "1.4.0" % "provided"

val sparkSql = "org.apache.spark" % "spark-sql_2.10" % "1.4.0" % "provided"

val graphx = "org.apache.spark" % "spark-graphx_2.10" % "1.4.0" % "provided"

val sparkCsv = "com.databricks" % "spark-csv_2.10" % "1.1.0"

val guava = "com.google.guava" % "guava" % "11.0"

val scalaTest =  "org.scalatest" % "scalatest_2.10" % "2.2.5"

libraryDependencies ++= Seq(
  sparkCore,
  sparkSql,
  graphx,
  sparkCsv,
  guava,
  scalaTest
)

test in assembly := {}