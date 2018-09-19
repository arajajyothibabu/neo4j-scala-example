
import models.{City, User}

object Main extends App {

  val jyothi = User("Jyothi", "Vijayawada")
  val kiran = User("Kiran", "Vizag")
  val atreya = User("Atreya", "Vizag")
  val sunil = User("Sunil", "Rajahmundry")
  val koushik = User("Koushik", "Vizag")
  val uma = User("Uma", "Kakinada")

  val vijayawada = City("Vijayawada")

  val users = List(kiran, atreya, sunil, koushik, uma)

  val neo4jDAO = new Neo4jDAO()

  neo4jDAO.createIndex("User", List("name"))
  neo4jDAO.createConstraint("City", "name")

  print(neo4jDAO.insertUser(jyothi))

  print(neo4jDAO.insertCity(vijayawada))

  users.map(neo4jDAO.insertUser)

  print(neo4jDAO.readUser(jyothi.name))

  neo4jDAO.removeUser(kiran.name)

}
