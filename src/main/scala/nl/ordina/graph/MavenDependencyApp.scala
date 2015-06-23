package nl.ordina.graph

import com.databricks.spark.csv._
import org.apache.spark.graphx._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object MavenDependencyApp {

  def main(args: Array[String]) {
    checkForValidInput(args)
    val sqlContext = initializeContext
    val data = readDataFromFile(args(0), sqlContext)
    val graph = constructGraphFromData(data)
    val ranks = getPageRanks(graph)
    displayPageRanks(ranks,graph.vertices)
    saveGraphToDisk(graph,args(1))
  }

  def checkForValidInput(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalStateException(
        """
          |Please supply the correct input:
          |   args[0]: File path of input file.
          |   args[1]: Output directory.
        """.stripMargin)
    }
  }

  private[this] def initializeContext: SQLContext = {
    val conf = new SparkConf().setAppName("Maven Dependency Graph")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext
  }

  private[this] def readDataFromFile(fileLocation: String, sqlContext: SQLContext): DataFrame = {
    sqlContext.tsvFile(fileLocation, useHeader = false)
  }

  private[this] def constructGraphFromData(data: DataFrame): Graph[MavenEntry, String] = {
    val tuples = data.map(rowToTupleOfMavenEntryAndListOfDependencies)
    tuples.cache()
    val vertices = tuples.map(tuple => (tuple._1.getUniqueId, tuple._1))
    val edges = tuples.flatMap(establishRelationships)
    Graph(vertices, edges)
  }

  private[this] def getPageRanks(graph: Graph[MavenEntry, String]): VertexRDD[Double] = {
    graph.pageRank(0.0001).vertices //FIXME magic number
  }

  private[this] def displayPageRanks(ranks: VertexRDD[Double],vertices: VertexRDD[MavenEntry]): Unit = {
    implicit val topRankOrdering = new Ordering[(MavenEntry, Double)] {
      override def compare(a: (MavenEntry, Double), b: (MavenEntry, Double)) = (b._2 - a._2).toInt
    }

    val ranksByMavenEntry = vertices.join(ranks).map {
      case (id, (mavenEntry, rank)) => (mavenEntry, rank)
    }
    ranksByMavenEntry.takeOrdered(25).zipWithIndex.foreach {
      case ((entry, rank), index) => println("" + index + ": " + entry + " with rank " + rank)
    }
  }

  private[this] def saveGraphToDisk(graph: Graph[MavenEntry, String], outputFolder: String) ={
    graph.vertices.saveAsTextFile(outputFolder)
    graph.edges.saveAsTextFile(outputFolder)
  }

  private[this] def establishRelationships(tuple: (MavenEntry, List[MavenEntry])): List[Edge[String]] = {
    val dependencyRelationship = "depends on"
    val entry = tuple._1
    tuple._2.map {
      dependency => Edge(entry.getUniqueId, dependency.getUniqueId, dependencyRelationship)
    }
  }

  private[this] def rowToTupleOfMavenEntryAndListOfDependencies(row: Row): (MavenEntry, List[MavenEntry]) = {
    val entry = MavenEntry(row.getString(0), row.getString(1), row.getString(2))
    val dependencies = MavenEntry.readBase64EncodedJSONtoListOfMavenEntries(row.getString(3))
    (entry, dependencies)
  }
}