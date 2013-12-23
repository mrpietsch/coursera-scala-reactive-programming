package kvstore

import akka.actor.{ActorRef, ActorLogging, Props, Actor}
import scala.util.Random
import java.util.concurrent.atomic.AtomicInteger

object Persistence {
  case class Persist(key: String, valueOption: Option[String], id: Long)
  case class Persisted(key: String, id: Long)

  class PersistenceException extends Exception("Persistence failure")

  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)
}

class Persistence(flaky: Boolean) extends Actor with ActorLogging {
  import Persistence._

  def receive = {
    case Persist(key, _, id) =>
      if (!flaky || Random.nextBoolean()) {
        val persisted: Persisted = Persisted(key, id)
        val ref: ActorRef = sender
        log.info(s"Sending $persisted from $self to $ref")
        ref ! persisted
      }
      else throw new PersistenceException
  }

}
