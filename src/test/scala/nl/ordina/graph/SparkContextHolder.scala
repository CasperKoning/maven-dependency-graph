package nl.ordina.graph

import org.apache.spark.SparkContext

object SparkContextHolder {
  val sc = new SparkContext("local","test")
}
