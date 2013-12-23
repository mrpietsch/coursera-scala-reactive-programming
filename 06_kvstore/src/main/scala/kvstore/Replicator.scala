package kvstore

import akka.actor._
import scala.concurrent.duration._
import scala.Some
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher


  // we want to receive Terminated messages but we won't process
  // them... hence we die with the replica...
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 100 milliseconds, loggingEnabled = true) {
    case _ => SupervisorStrategy.escalate
  }
  context.watch(replica)


  context.system.scheduler.schedule(0 seconds, 100 milliseconds, self, "processBatch")

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

    case "processBatch" =>
      pending.foreach( replica ! _)

    case r@Replicate(key, valueOption, id) =>
      val seq = nextSeq
      acks = acks + (seq ->(sender, r))
      val snapshot: Snapshot = Snapshot(key, valueOption, seq)
      pending = pending :+ snapshot

    case SnapshotAck(key, seq) =>
      acks.get(seq) match {
        case Some((senderRef, originalReplicateRequest)) =>
          if ( pending.head.seq == seq) {
            pending = pending.tail
            acks = acks - seq
            senderRef ! Replicated(originalReplicateRequest.key, originalReplicateRequest.id)
          }

        case None => // todo oder oben mit ask pattern arbeiten
      }

    case ReceiveTimeout => log.info("TIMEOUT")
  }

}
