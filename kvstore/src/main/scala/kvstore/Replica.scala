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
import java.util.concurrent.TimeUnit

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

  case class PersistRetry(id: Long)
  case class ReplicatorRetry(id: Long, replicator: ActorRef)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // map ( of not yet ack ) from id to pair of sender and request
  var persistenceAcks = Map.empty[Long, (ActorRef, Persist)]
  // map ( of not yet ack ) from (id, replicator) to (sender, request )
  var replicatorAcks = Map.empty[(Long, ActorRef), (ActorRef, Replicate)]

  var persistence = context.actorOf(persistenceProps)
  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {

    case Insert(key, value, id) => {
      kv = kv + (key -> value)
      persistWithAck(sender, Persist(key, Some(value), id))
      replicateWithAck(sender, Replicate(key, Some(value), id))
      scheduleAckTimeout(id)
    }

    case Remove(key, id) => {
      kv = kv - key
      persistWithAck(sender, Persist(key, None, id))
      replicateWithAck(sender, Replicate(key, None, id))
      scheduleAckTimeout(id)
    }

    case failed @ OperationFailed(id) => {
      persistenceAcks.get(id).map {
        case (sender, _) => {
          sender ! failed
          persistenceAcks = persistenceAcks - id
        }
      }

      replicatorAcks.find { case (id, _) => true }.map {
        case ((id, _), (sender, _)) => {
          sender ! failed
          replicatorAcks = replicatorAcks.filterKeys { case (id, _) => false }
        }
      }
    }

    case ReplicatorRetry(id, replicator) => {
      propagateChangesToReplicator(id, replicator)
    }

    case PersistRetry(id) => {
      propagateChangesToPersistence(id)
    }

    case Persisted(key, id) => {
      val client = persistAck(id)
      if (!replicatorAcks.exists { case (id, _) => true }) client ! OperationAck(id)
    }

    case Replicated(key, id) => {
      replicateAck(id, sender).map {
        case client => if (!persistenceAcks.contains(id) && !replicatorAcks.exists { case (id, _) => true }) client ! OperationAck(id)
      }

    }

    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case Replicas(replicas) => {

      def isGone(replica: ActorRef) = secondaries.contains(replica) && !replicas.contains(replica)
      def isNew(replica: ActorRef) = !secondaries.contains(replica) && replicas.contains(replica)

      for (replica <- (replicas - self) ++ secondaries.keys.toSet) {
        if (isGone(replica)) {
          secondaries(replica) ! PoisonPill
          secondaries -= replica
        } else if (isNew(replica)) {
          val replicator = context.actorOf(Replicator.props(replica))
          secondaries += ((replica, replicator))
          replicators += replicator

          kv.foldLeft(0) {
            case (seq, (key, value)) =>
              replicator ! Replicate(key, Some(value), seq)
              seq + 1
          }
        }
      }
    }
  }

  var expectedSeqNumber: Long = 0
  def nextSeq = {
    expectedSeqNumber += 1
  }

  def scheduleAckTimeout(seq: Long) {
    context.system.scheduler.scheduleOnce(Duration(1, TimeUnit.SECONDS), self, OperationFailed(seq));
  }

  def replicateWithAck(sender: ActorRef, replicate: Replicate) {
    replicators.foreach(replicator => {
      replicatorAcks += (replicate.id, replicator) -> (sender, replicate)
      propagateChangesToReplicator(replicate.id, replicator)
    })
  }

  def replicateAck(id: Long, replicator: ActorRef): Option[ActorRef] = {
    replicatorAcks.get((id, replicator)).map {
      case (client, _) => {
        replicatorAcks = replicatorAcks - ((id, replicator))
        client
      }
    }
  }

  def propagateChangesToReplicator(id: Long, replicator: ActorRef) {
    replicatorAcks.get((id, replicator)).map {
      case (_, replicate) => {
        replicator ! replicate
        context.system.scheduler.scheduleOnce(Duration(100, TimeUnit.MILLISECONDS), self, ReplicatorRetry(id, replicator))
      }
    }
  }

  def persistWithAck(sender: ActorRef, persist: Persist) {
    persistenceAcks = persistenceAcks + (persist.id -> (sender, persist))
    propagateChangesToPersistence(persist.id)
  }

  def persistAck(id: Long): ActorRef = {
    val (sender, _) = persistenceAcks(id)
    persistenceAcks = persistenceAcks - id
    sender
  }

  def propagateChangesToPersistence(seq: Long) {
    persistenceAcks.get(seq).map {
      case (sender, persist) => {
        persistence ! persist
        context.system.scheduler.scheduleOnce(Duration(100, TimeUnit.MILLISECONDS), self, PersistRetry(seq))
      }
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case Snapshot(key, valueOption, seq) => {
      // When a snapshot arrives at a Replica with a sequence number which is greater 
      // than the currently expected number, then that snapshot must be ignored (meaning no state change and no reaction).
      if (seq > expectedSeqNumber) {}
      // when a snapshot arrives at a Replica with a sequence number which is smaller than the currently expected number,
      // then that snapshot must be ignored and immediately acknowledged as described below.
      else if (seq < expectedSeqNumber) {
        sender ! SnapshotAck(key, seq)
      } else {
        valueOption match {
          case None    => kv = kv - key
          case Some(v) => kv = kv + (key -> v)
        }
        nextSeq
        persistWithAck(sender, Persist(key, valueOption, seq))
      }
    }

    case PersistRetry(seq) => {
      propagateChangesToPersistence(seq)
    }

    case Persisted(key, seq) => {
      val client = persistAck(seq)
      client ! SnapshotAck(key, seq)
    }

  }

}

