package nl.ordina.graph

import java.nio.charset.Charset
import java.util.Base64

import com.google.common.hash._
import org.apache.spark.sql._
import spray.json._

case class MavenEntry(groupId: Option[String] = Some("NO_GROUP_ID"), artifactId: Option[String] = Some("NO_ARTIFACT_ID"), version: Option[String] = Some("NO_VERSION")) {
  def getUniqueId: Long = {
    val hasher = Hashing.goodFastHash(64).newHasher()

    hasher.putString(groupId.getOrElse("NO_GROUP_ID"), Charset.defaultCharset())
    hasher.putString(artifactId.getOrElse("NO_ARTIFACT_ID"), Charset.defaultCharset())
    hasher.putString(version.getOrElse("NO_VERSION"), Charset.defaultCharset())

    hasher.hash().asLong()
  }
}

object MavenEntry extends DefaultJsonProtocol {
  implicit val mavenEntryFormat = jsonFormat(MavenEntry.apply, "groupid", "artifactid", "version")
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
    json.parseJson.convertTo[List[MavenEntry]]
  }

  def getDependenciesFromRow(row: Row): List[MavenEntry] = {
    MavenEntry.readBase64EncodedJSONtoListOfMavenEntries(row.getString(3))
  }

  def getFirstMavenEntryFromRow(row: Row): MavenEntry = {
    MavenEntry(Option(row.getString(0)), Option(row.getString(1)), Option(row.getString(2)))
  }
}
