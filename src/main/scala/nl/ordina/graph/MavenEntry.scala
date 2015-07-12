package nl.ordina.graph

import java.util.Base64

import com.google.common.hash.{Hasher, Hashing}
import org.apache.spark.sql.Row

import scala.util.parsing.json.JSON

case class MavenEntry(groupId: String, artifactId: String, version: String) {
  /**
   * I return a unique Id, defined by my state, in the shape of a Long.
   */
  def getUniqueId: Long = {
    val hasher = Hashing.goodFastHash(64).newHasher()

    if (groupId != null && !groupId.isEmpty) hasher.putString(groupId)
    if (artifactId != null && !artifactId.isEmpty) hasher.putString(artifactId)
    if (version != null && !version.isEmpty) hasher.putString(version)

    hasher.hash().asLong()
  }
}

object MavenEntry {
  /**
   * Utility method for reading a Base64 encoded String that contains a JSON-array of MavenEntries.
   * For example, the Base64 String:
   *
   * W3sidmVyc2lvbiI6ICIwLjMuMCIsICJncm91cGlkIjogIm9yZy5zYWRpZnJhbWV3b3JrIiwgImFydGlmYWN0aWQiOiAic2FkaS1zZXJ2aWNlIn0sIH
   * sidmVyc2lvbiI6ICIyLjAiLCAiZ3JvdXBpZCI6ICJvcmcuYXBhY2hlLm1hdmVuIiwgImFydGlmYWN0aWQiOiAibWF2ZW4tcGx1Z2luLWFwaSJ9LCB7I
   * nZlcnNpb24iOiAiMi4wIiwgImdyb3VwaWQiOiAib3JnLmFwYWNoZS5tYXZlbiIsICJhcnRpZmFjdGlkIjogIm1hdmVuLXByb2plY3QifSwgeyJ2ZXJz
   * aW9uIjogIjEuNi4yIiwgImdyb3VwaWQiOiAib3JnLmFwYWNoZS52ZWxvY2l0eSIsICJhcnRpZmFjdGlkIjogInZlbG9jaXR5In1d
   *
   * encodes the JSON
   *
   * [{"version": "0.3.0", "groupid": "org.sadiframework", "artifactid": "sadi-service"}, {"version": "2.0", "groupid":
   * "org.apache.maven", "artifactid": "maven-plugin-api"}, {"version": "2.0", "groupid": "org.apache.maven",
   * "artifactid": "maven-project"}, {"version": "1.6.2", "groupid": "org.apache.velocity", "artifactid": "velocity"}]
   *
   * which yields the list of MavenEntries
   *
   * List(MavenEntry(org.sadiframework,sadi-service,0.3.0), MavenEntry(org.apache.maven,maven-plugin-api,2.0),
   * MavenEntry(org.apache.maven,maven-project,2.0), MavenEntry(org.apache.velocity,velocity,1.6.2))
   *
   * @param base64EncodedJSON the encoded string
   * @return A list of MavenEntries that were encoded in the string
   */
  def readBase64EncodedJSONtoListOfMavenEntries(base64EncodedJSON: String): List[MavenEntry] = {
    val json = new String(Base64.getDecoder.decode(base64EncodedJSON))
    JSON.parseFull(json) match {
      case Some(x: List[Map[String, String]]) => x map getMavenEntry
      case _ => List()
    }
  }

  private[this] def getMavenEntry(mavenMap: Map[String, String]): MavenEntry = {
    MavenEntry(
      mavenMap.getOrElse("groupid", "NO_GROUP_ID"),
      mavenMap.getOrElse("artifactid", "NO_ARTIFACT_ID"),
      mavenMap.getOrElse("version", "NO_VERSION")
    )
  }

  def getDependenciesFromRow(row: Row): List[MavenEntry] = {
    MavenEntry.readBase64EncodedJSONtoListOfMavenEntries(row.getString(3))
  }

  def getFirstMavenEntryFromRow(row: Row): MavenEntry = {
    MavenEntry(row.getString(0), row.getString(1), row.getString(2))
  }
}