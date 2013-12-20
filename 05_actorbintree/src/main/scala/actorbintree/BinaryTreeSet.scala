/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue
import scala.None

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC =>
      log.info("FULL GC")
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot), false)
      root ! CopyTo(newRoot)

    case op : Operation => root forward op
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {

    case GC => ()
    // already doing as I was told... don't bother!

    case CopyFinished =>
      // stop the old actors
      root ! PoisonPill
      // replay all pending operations
      pendingQueue.foreach(op => newRoot ! op)
      pendingQueue = Queue.empty[Operation]
      // install the new root
      root = newRoot
      // become normal
      context.unbecome()

    case op: Operation => pendingQueue = pendingQueue.enqueue(op)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional


  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case CopyTo(target) =>

      // if not marked removed, copy myself to the target
      if ( ! removed ) {
        target ! Insert(self, -1, elem)
      }
      // wait for the finished message
      val children: Set[ActorRef] = subtrees.values.toSet
      children.foreach(child => child ! CopyTo(target))

      if ( removed && children.isEmpty) {
        sender ! CopyFinished
      } else {
        context.become(copying(children, removed))
      }


    case Insert(req, id, e) =>
      if (elem == e) {
        // found the element... make sure it is not marked removed...
        removed = false
        log.info(s"Sending insert ${OperationFinished(id)}")
        req ! OperationFinished(id)
      } else {
        // depending on the element to search descend to left or right
        // if the according branch is not available we can insert the element right there
        val positionToContinue = if (e < elem) Left else Right
        val childRefOption = subtrees.get(positionToContinue)

        childRefOption match {
          case None =>
            subtrees = subtrees.updated(positionToContinue, context.actorOf(props(e, false)))
            log.info(s"Sending insert ${OperationFinished(id)}")
            req ! OperationFinished(id)
          case Some(childRef) =>
            childRef ! Insert(req, id, e)
        }
      }

    case Contains(req, id, e) =>
      if (elem == e) {
        // found the element
        // nevertheless check if it has been marked remove meanwhile
        // finish the recursion and tell the requester
        log.info(s"Sending Contains ${ContainsResult(id, !removed)}")
        req ! ContainsResult(id, !removed)
      } else {
        // depending on the element to search descend to left or right
        // if the according branch is not available we can finish and return false
        val positionToContinue = if (e < elem) Left else Right
        val childRefOption = subtrees.get(positionToContinue)

        childRefOption match {
          case None =>
            log.info(s"Sending Contains ${ContainsResult(id, false)}")
            req ! ContainsResult(id, false)
          case Some(childRef) => childRef ! Contains(req, id, e)
        }
      }

    case Remove(req, id, e) =>
      if (elem == e) {
        // found the element... marke removed...
        removed = true
        log.info(s"Sending remove ${OperationFinished(id)}")
        req ! OperationFinished(id)
      } else {
        // depending on the element to search descend to left or right
        // if the according branch is not available we can insert the element right there
        val positionToContinue = if (e < elem) Left else Right
        val childRefOption = subtrees.get(positionToContinue)

        childRefOption match {
          case None =>
            // nothing to remove here... just quit
            log.info(s"Sending remove ${OperationFinished(id)}")
            req ! OperationFinished(id)
          case Some(childRef) =>
            childRef ! Remove(req, id, e)
        }
      }

  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {

    case CopyFinished => checkFinishedCopying(expected - sender, true)

    case OperationFinished(_) => checkFinishedCopying(expected, true)

  }

  def checkFinishedCopying(expected: Set[ActorRef], insertConfirmed: Boolean) {
    if (expected.isEmpty && insertConfirmed) {
      context.parent ! CopyFinished
      context.unbecome()
    } else {
      context.become(copying(expected, true))
    }
  }

}
