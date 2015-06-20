package nl.ordina.graph

import com.databricks.spark.csv._
import org.apache.spark.graphx._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object MavenDependencyApp {
  implicit val topRankOrdering = new Ordering[(MavenEntry, Double)] {
    override def compare(a: (MavenEntry, Double), b: (MavenEntry, Double)) = (b._2 - a._2).toInt
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Maven Dependency Graph")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.tsvFile(args(0), useHeader = false)
    val tuples = data.map(rowToTupleOfMavenEntryAndListOfDependencies)
    tuples.cache()
    val vertices = tuples.map(tuple => (tuple._1.getUniqueId, tuple._1))
    val edges = tuples.flatMap(establishRelationships)
    val graph: Graph[MavenEntry, String] = Graph(vertices, edges)
    val ranks = graph.pageRank(0.0001).vertices //FIXME magic number
    val ranksByMavenEntry = vertices.join(ranks).map{
        case (id, (mavenEntry, rank)) => (mavenEntry, rank)
      }
    ranksByMavenEntry.takeOrdered(25).zipWithIndex.foreach{
      case ((entry,rank),index) => println(""+index+": " + entry + " with rank " + rank)
    }
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