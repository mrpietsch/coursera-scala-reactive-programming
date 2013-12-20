package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

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

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  arbiter ! Join

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var expectedSeq : Long = 0
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
//  var replicators = Set.empty[ActorRef]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Insert(key, value, id) =>

      kv = kv + (key -> value)
      // inform our replicators of the change
      secondaries.values foreach (_ ! Replicate(key, Some(value), id))

      sender ! OperationAck(id)

    case Remove(key, id) =>
      kv = kv - key
      // inform our replicators of the change
      secondaries.values foreach (_ ! Replicate(key, None, id))

      sender ! OperationAck(id)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(newReplicaSet) =>
      // skip us
      val secondaryReplicas = newReplicaSet.filter( _ != self )

      // calculate the difference
      val newReplicas = secondaryReplicas -- secondaries.keySet
      val deletedReplicas = secondaries.keySet -- secondaryReplicas

      // stop all replicators whose replicas stopped working
      deletedReplicas.flatMap(secondaries.get).foreach(context.stop)

      // start replicator foreach new replica a remember the mapping
      // todo synchronize with all data
      val newReplicatorMappings = newReplicas.map( replica => (replica, context.actorOf(Replicator.props(replica))))

      newReplicatorMappings.map(_._2).foreach {
        r =>
      }

      // update our mapping
      secondaries = (secondaries -- deletedReplicas) ++ newReplicatorMappings

  }

  val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) if seq > expectedSeq  =>
    case Snapshot(key, valueOption, seq) if seq < expectedSeq  => sender ! SnapshotAck(key, seq)
    case Snapshot(key, valueOption, seq) if seq == expectedSeq  =>
        valueOption match {
          case Some(value) => kv = kv + (key -> value)
          case None => kv = kv - key
        }

        expectedSeq = seq + 1
        sender ! SnapshotAck(key, seq)


    // case Replicas(newReplicators) => replicators = newReplicators
    // case Insert(key, value, id) =>
    // case Remove(key, id) =>
  }

}
