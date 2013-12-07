/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import akka.event.LoggingReceive

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

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with Stash {

  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive {
    case msg: Operation => root ! msg
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(
        garbageCollecting(newRoot)
      )
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = LoggingReceive {
    case msg: Operation => stash()
    case CopyFinished => {
      context.stop(root)
      root = newRoot
      unstashAll()
      context.become(normal)
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, val initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  def createNode(e: Int) = context.actorOf(BinaryTreeNode.props(e, false))

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def forwardToChild(
                      msg: Operation,
                      onSome: (Operation, Position, ActorRef) => Unit,
                      onNone: (Operation, Position) => Unit
                    ) = {
    val pos = if (msg.elem < elem) Left else Right
    subtrees.get(pos) match {
      case None => onNone(msg, pos)
      case Some(child) => onSome(msg, pos, child)
    }
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = LoggingReceive {

    case msg: Insert if msg.elem == elem => {
      removed = false
      msg.requester ! OperationFinished(msg.id)
    }
    case msg: Insert => {
      forwardToChild(
        msg,
        (msg, _, child) => child ! msg,
        (msg, pos) => {
          subtrees = subtrees.updated(pos, createNode(msg.elem))
          msg.requester ! OperationFinished(msg.id)
        }
      )
    }

    case msg: Contains if msg.elem == elem => {
      msg.requester ! ContainsResult(msg.id, !removed)
    }
    case msg: Contains => {
      forwardToChild(
        msg,
        (msg, _, child) => child ! msg,
        (msg, _) => msg.requester ! ContainsResult(msg.id, false)
      )
    }

    case msg: Remove if msg.elem == elem => {
      removed = true
      msg.requester ! OperationFinished(msg.id)
    }
    case msg: Remove => {
      forwardToChild(
        msg,
        (msg, _, child) => child ! msg,
        (msg, _) => msg.requester ! OperationFinished(msg.id)
      )
    }

    case msg: CopyTo => {
      val id = self.hashCode()

      val children = subtrees.values.toSet
      children.foreach { _ ! msg }

      if (!removed) msg.treeNode ! Insert(self, id, elem)

      context.become(
        children match {
          case s if s.isEmpty && removed => context.parent ! CopyFinished; normal
          case s => copying(s, id, removed)
        }
      )
    }

  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertId: Int, insertConfirmed: Boolean): Receive = LoggingReceive {
    case OperationFinished(`insertId`) => {
      context.become(
        expected match {
          case s if s.isEmpty => context.parent ! CopyFinished; normal
          case s => copying(s, insertId, true)
        }
      )
    }
    case CopyFinished => {
      context.become(
        expected - sender match {
          case s if s.isEmpty && insertConfirmed => context.parent ! CopyFinished; normal
          case s => copying(s, insertId, insertConfirmed)
        }
      )
    }
  }

}
