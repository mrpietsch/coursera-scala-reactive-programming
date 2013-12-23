package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.language.postfixOps
import scala.Some
import akka.actor.OneForOneStrategy
import kvstore.Arbiter.Replicas
import akka.actor.Terminated
import scala.concurrent.Future
import kvstore.Replicator.Replicate
import scala.util.{Failure, Success}

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher


  override val supervisorStrategy = OneForOneStrategy(-1, 100 milliseconds, loggingEnabled = true) {
    case _: Exception => SupervisorStrategy.Restart
  }

  var persistence = context.actorOf(persistenceProps)
  context.watch(persistence)

  log.info(s"$self is trying to join the cluster")

  arbiter ! Join

  var expectedSeq : Long = 0
  var kv = Map.empty[String, String]

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  // outstanding ackknowledgements
  var owedSnapshotAcks = Map.empty[Long, (Long, String, ActorRef)] // seq -> (maxTime, key, originalSender)
  var owedOperationAcks = Map.empty[Long, (Long, ActorRef)] // id -> (maxTime, originalSender)
  var expectedAcknowledgementsReplication = Map.empty[Long,Set[ActorRef]]
  var expectedAcknowledgementsPersistence = Map.empty[Long, Persist]

  def receive = {
    case JoinedPrimary   =>
      context.system.scheduler.schedule(0 seconds, 100 milliseconds, self, "processAllOwedOperationAcks")
      context.become(leader)
    case JoinedSecondary =>
      context.system.scheduler.schedule(0 seconds, 100 milliseconds, self, "processAllOwedSnapshotAcks")
      context.become(replica)
  }

  val leader: Receive = {
    case "processAllOwedOperationAcks" =>
      owedOperationAcks.keys.foreach(checkGlobalAcknowledgementLeader)
      
    case Insert(key, value, id) =>
      kv = kv + (key -> value)
      replicateAndPersistLeader(sender, key, Some(value), id)

    case Persisted(key,id) =>
      log.info(s"Leader Persisted message $id for key $key")
      expectedAcknowledgementsPersistence = expectedAcknowledgementsPersistence - id
      // check if the replication is also ready and acknowledge if need be
      checkGlobalAcknowledgementLeader(id)

    case Replicated(key, id) =>
      val replicator = sender
      log.info(s"Leader Replicated message $id for key $key")
      expectedAcknowledgementsReplication.get(id) match  {
        case None => // ignored... OperationFailed has already been sent
        case Some(actorRefSet) =>
          val newActorRefSet = actorRefSet - replicator
          expectedAcknowledgementsReplication = expectedAcknowledgementsReplication.updated(id, newActorRefSet)
          if ( actorRefSet.isEmpty ) {
            // check if the persistence is also ready and acknowledge if need be
            checkGlobalAcknowledgementLeader(id)
          }
      }

    case Remove(key, id) =>
      kv = kv - key
      replicateAndPersistLeader(sender, key, None, id)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(newReplicaSet) =>
      // skip us since we are the primary
      val secondaryReplicas = newReplicaSet.filter( _ != self )

      // calculate the difference to what we know
      val newReplicas = secondaryReplicas -- secondaries.keySet
      val deletedReplicas = secondaries.keySet -- secondaryReplicas

      // stop all replicators whose replicas stopped working
      deletedReplicas.flatMap(secondaries.get).foreach(context.stop)

      // start replicator foreach new replica and remember the mapping
      val newReplicatorMappings = for {
        replica <- newReplicas
        newReplicator = context.actorOf(Replicator.props(replica))
      } yield (replica, newReplicator)

      // resend updates for all key-value tuple we know of using negative IDs
      // to distinguish them from real updates
      for {
        (_, replicator) <- newReplicatorMappings
        ((key, value), id) <- kv.zipWithIndex
      } replicator ! Replicate(key, Some(value), -id)

      // update our mapping
      secondaries = (secondaries -- deletedReplicas) ++ newReplicatorMappings

    case Terminated(p) => log.info(s"Termination message from $p")
  }

  val replica: Receive = {

    case "processAllOwedSnapshotAcks" =>
      owedSnapshotAcks.keys.foreach(checkGlobalAcknowledgementReplica)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Persisted(key, id) =>
      expectedAcknowledgementsPersistence = expectedAcknowledgementsPersistence - id
      checkGlobalAcknowledgementReplica(id)

    case sn@Snapshot(key, valueOption, seq) if seq > expectedSeq =>
      log.info(s"Expecting sequence number $expectedSeq... Dropping future $sn")

    case sn@Snapshot(key, valueOption, seq) if seq < expectedSeq =>
      log.info(s"$sn has already been processed... Expecting sequence number $expectedSeq... Acknowledging again...")
      sender ! SnapshotAck(key, seq)

    case sn@Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      valueOption match {
        case Some(value) => kv = kv + (key -> value)
        case None => kv = kv - key
      }
      expectedSeq = seq + 1

      // remember that we have to send a SnapshotAck
      owedSnapshotAcks = owedSnapshotAcks + (seq -> (calculateMaxTime, key, sender))
      registerPersistRequest(key, valueOption, seq)
      checkGlobalAcknowledgementReplica(seq)
  }

  def checkGlobalAcknowledgementReplica(seq: Long) = {
    owedSnapshotAcks.get(seq) match {
      case None => log.info(s"$seq not found... no outstanding replication acks")
      case Some((maxTime, key, originalSender)) =>

        if (!expectedAcknowledgementsPersistence.contains(seq)) {
          originalSender ! SnapshotAck(key, seq)
        }
        else {
          if (System.currentTimeMillis() > maxTime) {
            expectedAcknowledgementsPersistence = expectedAcknowledgementsPersistence - seq
            owedSnapshotAcks = owedSnapshotAcks - seq
          } else {
            // still in time, retry to persist if this was the error
            expectedAcknowledgementsPersistence.get(seq) match {
              case None => // nope, still waiting for the replication but not for the persistence
              case Some(persistRequest) =>
                // send persistence request
                log.info(s"Sending $persistRequest as $self to $persistence")
                persistence ! persistRequest
            }
          }
        }

    }
  }

  def checkGlobalAcknowledgementLeader(id: Long) = {
    owedOperationAcks.get(id) match {
      case None => log.info(s"$id not found in `owedOperationAcks`")
      case Some((maxTime, originalSender)) =>
        if (!expectedAcknowledgementsPersistence.contains(id) && expectedAcknowledgementsReplication.getOrElse(id, Set()).isEmpty) {
          originalSender ! OperationAck(id)
          expectedAcknowledgementsPersistence = expectedAcknowledgementsPersistence - id
          expectedAcknowledgementsReplication = expectedAcknowledgementsReplication - id
          owedOperationAcks = owedOperationAcks - id
        } else {
          if (System.currentTimeMillis() > maxTime) {
            originalSender ! OperationFailed(id)
            expectedAcknowledgementsPersistence = expectedAcknowledgementsPersistence - id
            expectedAcknowledgementsReplication = expectedAcknowledgementsReplication - id
            owedOperationAcks = owedOperationAcks - id
          } else {
            // still in time, retry to persist if this was the error
            expectedAcknowledgementsPersistence.get(id) match {
              case None => // nope, still waiting for the replication but not for the persistence
              case Some(persistRequest) => persistence ! persistRequest
            }
          }
        }
    }
  }

  def calculateMaxTime = System.currentTimeMillis() + 1000

  def replicateAndPersistLeader(ackReceiver : ActorRef, key: String, valueOption: Option[String], id: Long) {
    // remember that we have to send an OperationReply
    owedOperationAcks = owedOperationAcks + (id -> (calculateMaxTime, sender))
    registerPersistRequest(key, valueOption, id)
    registerReplicationRequest(key, valueOption, id)
    checkGlobalAcknowledgementLeader(id)
  }

  def registerReplicationRequest(key: String, valueOption: Option[String], id: Long) = {
    val replicators = secondaries.values
    // send replication request to all replicators
    replicators.foreach(_ ! Replicate(key, valueOption, id))
    // remember that we are waiting for an answer
    expectedAcknowledgementsReplication = expectedAcknowledgementsReplication + (id -> replicators.toSet)
  }

  def registerPersistRequest(key: String, valueOption: Option[String], id: Long) {
    val persistRequest: Persist = Persist(key, valueOption, id)
    // remember that we are waiting for an answer
    expectedAcknowledgementsPersistence = expectedAcknowledgementsPersistence + (id -> persistRequest)
  }

}
