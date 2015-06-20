package nl.ordina.graph

import com.databricks.spark.csv._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest._


class MavenEntryTest extends FlatSpec with Matchers{
  "The number of distinct IDs" should "be equal to the number of distinct MavenEntries" in {
    val sc = new SparkContext("local","test")
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.tsvFile(this.getClass.getResource("/glassfishEntries.csv").getPath, useHeader = false) //glassfish is the most occuring groupId
    val mavenEntries = data.map { row =>
      MavenEntry(row.getString(0), row.getString(1), row.getString(2))
    }
    val ids = mavenEntries.map(entry => entry.getUniqueId)
    ids.distinct().count() should be (mavenEntries.distinct().count())
  }
}
