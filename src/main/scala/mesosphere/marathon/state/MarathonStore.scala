package mesosphere.marathon.state

import com.google.protobuf.InvalidProtocolBufferException
import org.apache.mesos.state.State
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration.Duration
import mesosphere.marathon.StorageException

/**
 * @author Tobi Knaup
 */

class MarathonStore[S <: MarathonState[_, S]](state: State,
                       newState: () => S, prefix:String = "app:") extends PersistenceStore[S] {

  val defaultWait = Duration(3, "seconds")

  import ExecutionContext.Implicits.global
  import mesosphere.util.BackToTheFuture._

  def fetch(key: String): Future[Option[S]] = {
    state.fetch(prefix + key) map {
      case Some(variable) => stateFromBytes(variable.value)
      case None => throw new StorageException(s"Failed to read $key")
    }
  }

  def modify(key: String)(f: (() => S) => S): Future[Option[S]] = {
    state.fetch(prefix + key) flatMap {
      case Some(variable) =>
        val deserialize = () => stateFromBytes(variable.value).getOrElse(newState())
        state.store(variable.mutate(f(deserialize).toProtoByteArray)) map {
          case Some(newVar) => stateFromBytes(newVar.value)
          case None => throw new StorageException(s"Failed to store $key")
        }
      case None => throw new StorageException(s"Failed to read $key")
    }
  }

  def expunge(key: String): Future[Boolean] = {
    state.fetch(prefix + key) flatMap {
      case Some(variable) =>
        state.expunge(variable) map {
          case Some(b) => b
          case None => throw new StorageException(s"Failed to expunge $key")
        }
      case None => throw new StorageException(s"Failed to read $key")
    }
  }

  def names(): Future[Iterator[String]] = {
    state.names().map {
      case Some(it) => it.asScala.collect {
        case name if name startsWith prefix =>
          name.replaceFirst(prefix, "")
      }
      case None => Seq().iterator
    } recover {
      // Thrown when node doesn't exist
      case e: ExecutionException => Seq().iterator
    }
  }

  private def stateFromBytes(bytes: Array[Byte]): Option[S] = {
    try {
      Some(newState().mergeFromProto(bytes))
    }
    catch {
      case e: InvalidProtocolBufferException => None
    }
  }
}
