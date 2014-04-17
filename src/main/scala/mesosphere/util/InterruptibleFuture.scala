package mesosphere.util

import scala.concurrent._
import java.util.concurrent.CancellationException

/**
 * Author: Shingo Omura
 */
object InterruptibleFuture {
  /**
   * This provides interruptible future by returning future itself and
   * a function which you can interrupt to the future's thread.
   *
   * example:
   *   import scala.concurrent._
   *   import mesosphere.util.InterruptibleFuture._
   *   import ExecutionContext.Implicits.global
   *
   *   val (f, interrupt) = interruptableFuture { Thread.sleep(1000) }
   *   try{
   *     Await.result(f, 10 seconds)
   *   } catch {
   *     case e => interrupt()
   *   }
   *
   * @param fun thunk
   * @param ec execution context
   * @tparam T return value type
   * @return (f, ()=>Boolean): f is future,
   *         ()=>Boolean is for interrupting the future.
   *         the interrupting function returns `false` if f has already been completed,
   *         `true` otherwise.
   */
  def interruptibleFuture[T](fun: => T)(implicit ec: ExecutionContext): (Future[T], () => Boolean) = {
    val p = Promise[T]()
    val lock = new Object
    var currentThread: Thread  = null

    def update(newThread: Thread): Thread = lock.synchronized {
      val old = currentThread
      currentThread = newThread
      old
    }

    p tryCompleteWith future {
      val thread = Thread.currentThread()
      update(thread)
      // you can interrupt during fun runs.
      try fun finally { update(null) }
    }

    (p.future , () => Option(update(null)) exists {
      t =>
        t.interrupt()
        p.tryFailure(new CancellationException)
    })
  }
}
