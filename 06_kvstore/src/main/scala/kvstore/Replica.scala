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
      replicateAndPersist(sender, key, Some(value), id)

    case Remove(key, id) =>
      kv = kv - key
      replicateAndPersist(sender, key, None, id)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicated(key, id) =>
      if ( id >= 0)

      log.info(s"replicated id=$id, key=$key")


    case Replicas(newReplicaSet) =>
      // skip us
      val secondaryReplicas = newReplicaSet.filter( _ != self )

      // calculate the difference
      val newReplicas = secondaryReplicas -- secondaries.keySet
      val deletedReplicas = secondaries.keySet -- secondaryReplicas

      // stop all replicators whose replicas stopped working
      deletedReplicas.flatMap(secondaries.get).foreach(context.stop)

      // start replicator foreach new replica a remember the mapping
      val newReplicatorMappings = newReplicas.map( replica => (replica, context.actorOf(Replicator.props(replica))))

      // todo synchronize with all data
      val indexedKeyValue: Map[(String, String), Long] = kv.zipWithIndex.mapValues( i => -i)
      val virginReplicators: Set[ActorRef] = newReplicatorMappings.map(_._2)

      for (
        repl <- virginReplicators;
        ((k, v), i) <- indexedKeyValue
      ) repl ! Replicate(k, Some(v), i)


      // update our mapping
      secondaries = (secondaries -- deletedReplicas) ++ newReplicatorMappings

    case Terminated(p) => log.info(s"Termination message from $p")
  }

  val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Persisted(k,s) => log.info(s"Persisted message $s for key $k ")

    case Snapshot(key, valueOption, seq) if seq > expectedSeq  =>
    case Snapshot(key, valueOption, seq) if seq < expectedSeq  => sender ! SnapshotAck(key, seq)
    case Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      valueOption match {
        case Some(value) => kv = kv + (key -> value)
        case None => kv = kv - key
      }
      expectedSeq = seq + 1

      val ackknowlegmentReceiver = sender

      val persistenceFuture: Future[Boolean] = persist(key, valueOption, seq, calculateMaxTime)

      persistenceFuture.onComplete {
        case Success(b) => {
          log.warning(s"Sending SnapshotAck ${b}")
          ackknowlegmentReceiver ! SnapshotAck(key, seq)
        }
        case Failure(e) =>log.warning(s"Failure ${e}")
      }


    // case Replicas(newReplicators) => replicators = newReplicators
    // case Insert(key, value, id) =>
    // case Remove(key, id) =>
  }

  def calculateMaxTime = System.currentTimeMillis() + 1000

  def replicateAndPersist(ackReceiver : ActorRef, key: String, valueOption: Option[String], id: Long) {

    val persistenceFuture: Future[Boolean] = persist(key, valueOption, id, calculateMaxTime)
    val replicationFuture: Future[Boolean] = replicate(key, valueOption, id)

    val allOkFuture: Future[Boolean] = Future.reduce(List(persistenceFuture, replicationFuture))( (a,b) => a && b)

    allOkFuture.onComplete {
      case Success(true) => ackReceiver ! OperationAck(id)
      case Success(false) => ackReceiver ! OperationFailed(id)
      case Failure(_) =>  ackReceiver ! OperationFailed(id)
    }
  }

  def replicate(key: String, valueOption: Option[String], id: Long) : Future[Boolean] = {

    implicit val timeout : Timeout = 1000.milliseconds

    val secondaryResultFutures: Iterable[Future[Boolean]] = secondaries.values.map {
      case sec: ActorRef =>
        (sec ? Replicate(key, valueOption, id))
          .mapTo[Replicated]
          .map {
          case Replicated(_, _) =>
            log.info(s"Replicated $key to $sec" )
            true
        }
          .recover {
          case e =>
            log.warning(s"Replication failed ${e.getMessage}")
            false
        }
    }

    Future.fold(secondaryResultFutures)(true)(_ && _)
  }

  /**
   * todo repeatedly try to persist
   * @param key key of the new entry
   * @param valueOption the value of the new entry, {None} if it shall be removed
   * @param id id of the request
   */
  def persist(key: String, valueOption: Option[String], id: Long, maxTime: Long): Future[Boolean] = {
    implicit val timeout : Timeout = 100.milliseconds

    val askFuture: Future[Any] = persistence ? Persist(key, valueOption, id)

    askFuture.mapTo[Persisted]
      .map(_ => true)
      .recoverWith {
      case e: Exception =>
        if (System.currentTimeMillis() < maxTime)  {
          log.error(s"Persistence failed... Retrying for $key... Cause ${e.getMessage}")
          persist( key, valueOption, id, maxTime)   }
        else {
          log.warning(s"Giving up to persist $key" )
          Future(false)
        }
    }
  }

}
