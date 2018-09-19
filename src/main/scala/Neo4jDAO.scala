
import models.{City, User}
import org.neo4j.driver.v1._

class Neo4jDAO(uri: String = "bolt://localhost/7687", username: String = "neo4j", password: String = "neo" ) {

  //Nodes
  val UserNode = "User"
  val CityNode = "City"

  //Relationships
  val LIVES_IN = "LIVES_IN"

  /**
    * Driver
    * @return
    */
  def getDriver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password))

  /**
    * Graph DB Session
    * @return
    */
  def getSession: Session = getDriver.session

  /**
    * Executes the query using driver session
    * @param query
    * @return
    */
  def executeQuery(query: String): StatementResult = {
    val session = getSession
    val result: StatementResult = session.run(query)
    session.close()
    result
  }

  //DAO Methods

  /**
    * One time action, Since Neo4j won't allow write operations with default credentials
    * @param password
    */
  def changePassword(password: String): Unit = {
    val query = s"CALL dbms.changePassword('$password')"
    executeQuery(query)
  }

  /**
    * Creates Constraint
    * @param nodeLabel
    * @param attribute
    * @return
    */
  def createConstraint(nodeLabel: String, attribute: String): Boolean = {
    val query = s"CREATE CONSTRAINT ON (node:$nodeLabel) ASSERT node.$attribute IS UNIQUE"
    val result = executeQuery(query)
    result.consume().counters().containsUpdates
  }

  /**
    * Creates Index
    * @param nodeLabel
    * @param attributes
    * @return
    */
  def createIndex(nodeLabel: String, attributes: List[String]): Boolean = {
    val query = s"CREATE INDEX ON :$nodeLabel(${attributes.mkString(", ")})"
    val result = executeQuery(query)
    result.consume().counters().containsUpdates
  }

  /**
    * Create operation
    * @param user
    * @return
    */
  def insertUser(user: User): Boolean = {
    val query = s"MERGE (city:$CityNode {name: '${user.city}'})" +
      s"MERGE (:$UserNode {name:'${user.name}'})-[:$LIVES_IN]->(city)"
    val result = executeQuery(query)
    result.consume().counters().containsUpdates
  }

  /**
    * Read operation
    * @param userName
    * @return
    */
  def readUser(userName: String): Option[User] = {
    val query = s"MATCH (user:$UserNode {name: '$userName'})-[:$LIVES_IN]->(city:$CityNode)" +
      s"RETURN user.name as name, city.name as city"
    val result = executeQuery(query)
    if(result.hasNext){ //Can have more users, this is just example :)
      val userRecord = result.next()
      Some(User(userRecord.get("name").asString(), userRecord.get("city").asString()))
    }else{
      None
    }
  }

  /**
    * Update operation
    * @param userName
    * @param city
    * @return
    */
  def updateUserCity(userName: String, city: String): Boolean = {
    val query = s"MATCH (:$UserNode {name:'$userName') as user" +
      s"OPTIONAL MATCH (user)-[relation:$LIVES_IN]->()" +
      s"DELETE relation" +
      s"MERGE (user)-[:$LIVES_IN]->(:$CityNode {name: '$city'})"
    val result = executeQuery(query)
    result.consume().counters().containsUpdates
  }

  /**
    * Delete operation
    * @param userName
    * @return
    */
  def removeUser(userName: String): Boolean = {
    val query = s"MATCH (user:$UserNode {name:'$userName'})" +
      s"OPTIONAL MATCH (user)-[relations]->()" +
      s"DELETE user, relations"
    val result = executeQuery(query)
    result.consume().counters().containsUpdates
  }

  def insertCity(city: City): Boolean = {
    val query = s"MERGE (:$CityNode {name:'${city.name}'})"
    val result = executeQuery(query)
    result.consume().counters().containsUpdates
  }

}
