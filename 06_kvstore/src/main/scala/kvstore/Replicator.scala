package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case r@Replicate(key: String, valueOption: Option[String], id: Long) =>
      val seq = nextSeq
      acks = acks + (seq ->(sender, r))
      replica ! Snapshot(key, valueOption, seq)

    case SnapshotAck(key: String, seq: Long) =>
      acks.get(seq) match {
        case Some((senderRef, originalReplicateRequest)) =>
          acks = acks - seq
          senderRef ! Replicated(originalReplicateRequest.key, originalReplicateRequest.id)
        case None => // todo
      }


  }

}
