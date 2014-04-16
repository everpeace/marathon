package mesosphere.util

import scala.language.postfixOps
import scala.language.implicitConversions
import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

object BackToTheFuture {

  import ExecutionContext.Implicits.global

  // TODO(everpeace) make it configurable or be declared outside of this object.
  implicit val _timeout = BackToTheFutureTimeout(2 seconds)
  case class BackToTheFutureTimeout(d:Duration)

  implicit def FutureToFutureOption[T](f: java.util.concurrent.Future[T])(implicit timeout:BackToTheFutureTimeout): Future[Option[T]] = {
    future {
      // f.get() blocks until the computation finished.
      // Particularly, if f is an future instance returned from AbstractState,
      // f.get could block forever this promise thread when Zookeeper was dead.
      // and this will also block MarathonSchedulerService's shutdown process.
      val t = f.get(timeout.d.toNanos, TimeUnit.NANOSECONDS)
      if (t == null) {
        None
      } else {
        Some(t)
      }
    }
  }

  implicit def ValueToFuture[T](value: T): Future[T] = {
    future {
      value
    }
  }
}
