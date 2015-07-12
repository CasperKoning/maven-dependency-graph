package nl.ordina.graph

import com.databricks.spark.csv._
import org.apache.spark.sql.SQLContext
import org.scalatest.{FlatSpec, Matchers}


class MavenDependencyAppSpec extends FlatSpec with Matchers {
  private val sqlContext = new SQLContext(SparkContextHolder.sc)
  private val data = sqlContext.tsvFile(this.getClass.getResource("/glassfishEntries.csv").getPath, useHeader = false).cache()//glassfish is the most occuring groupId
  private val mavenEntries = data.map(MavenEntry.getFirstMavenEntryFromRow)
  private val dependencies = data.flatMap(MavenEntry.getDependenciesFromRow)

  "The number of distinct IDs" should "be equal to the number of distinct MavenEntries" in {
    val idsEntries = mavenEntries.map(entry => entry.getUniqueId)
    val idsDependencies = dependencies.map(entry => entry.getUniqueId)
    val ids = idsEntries.union(idsDependencies)
    ids.distinct().count() should be(mavenEntries.union(dependencies).distinct().count())
  }

  "All of the read entries" should "not be null" in {
    mavenEntries.collect().foreach(entry => entry should not be null)
  }
  they should "not have a toString method that results in null" in {
    mavenEntries.collect().foreach(entry => entry.toString should not be null)
  }

  "All of the read dependencies" should "not be null" in {
    dependencies.collect().foreach(entry => entry should not be null)
  }
  they should "not have a toString method that results in null" in {
    dependencies.collect().foreach(entry => entry.toString should not be null)
  }
}
