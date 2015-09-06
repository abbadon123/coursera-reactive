/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

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


class BinaryTreeSet extends Actor {
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

    case operation:Operation => root ! operation 
    
    case GC => {
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
    }
    
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    // messages go to pendingQueue
    case op: Operation => pendingQueue = pendingQueue :+ op
    
    // GC is ignored
    case GC => None
    
    // when finished need to pass all messages to newRoot
    // and sucide old root
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      
      pendingQueue.foreach { root ! _ }
      pendingQueue = Queue.empty[Operation]
    
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

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    
    case operation: Operation if (operation.elem == elem) => {
        operation match {
          case Insert(_,_,_) => {
            removed = false
            operation.requester ! OperationFinished(operation.id)
          }
          case Remove(_,_,_) => {
            removed = true
            operation.requester ! OperationFinished(operation.id)
          }
          case Contains(_,_,_) => {
            operation.requester ! ContainsResult(operation.id, !removed)
          }
      }
    }
    
    case operation: Operation if (subtrees.contains( positionOf(operation.elem) )) => subtrees( positionOf(operation.elem) ) ! operation
 
    case insert @ Insert(requester, id, newElem) => {
      val node = newNode(id, newElem)
      subtrees = subtrees + (positionOf(newElem) -> node) 
      node ! insert
    }
    
    case remove @ Remove(request, id, toBeRemoved) => {
      request ! OperationFinished(id)
    }
    
    case contains @ Contains(request, id, el) => {
      request ! ContainsResult(id, false)
    }
    
    case CopyTo(newRoot) => {
      val subNodes = subtrees.values.toSet
      context.become(copying(subNodes, removed))
      if(!removed) newRoot ! Insert(self, -666, elem) // any id beacuse there will be at most one OperationFinished send to us
      subNodes.foreach { _ ! CopyTo(newRoot) }
    }
    
  }
  
  def newNode(id:Int, elem: Int): ActorRef = {
    context.actorOf(props(elem, false), "n=" + id + ",v=" + elem)
  }

  def positionOf(e: Int): Position = if (e < elem) Left else Right
  
  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    if (expected.isEmpty && insertConfirmed) { 
      context.parent ! CopyFinished 
      normal
    }
    else {
      case OperationFinished(id) => context.become(copying(expected, true))
      case CopyFinished =>  {
       context.become(copying(expected - sender, insertConfirmed))
       sender ! PoisonPill
      }
    }
  }


}