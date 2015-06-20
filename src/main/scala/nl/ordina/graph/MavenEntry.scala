package nl.ordina.graph

import java.util.Base64

import scala.util.parsing.json.JSON

case class MavenEntry(groupId: String, artifactId: String, version: String) {
  /**
   * I return a unique Id in the shape of a Long.
   *
   * Currently, the hashcode function is used to obtain the unique Id. Since the hashcode function returns an Int in
   * stead of a Long, there is room for improvement.
   *
   * Although some initial tests did not show any improvement using this algorithm, a potentially better candidate is
   * found in: http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
   */
  def getUniqueId: Long = {
    hashCode()
  }
}

object MavenEntry{
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
      case Some(x: List[Map[String, String]]) =>
        x.map(entry => MavenEntry(entry.get("groupid").get, entry.get("artifactid").get, entry.get("version").get))
      case _ => List()
    }
  }
}