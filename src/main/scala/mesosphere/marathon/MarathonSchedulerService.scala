package mesosphere.marathon

import scala.language.postfixOps
import org.apache.mesos.Protos.{TaskID, FrameworkInfo}
import org.apache.mesos.MesosSchedulerDriver
import java.util.logging.{Level, Logger}
import mesosphere.marathon.api.v1.AppDefinition
import mesosphere.marathon.api.v2.AppUpdate
import mesosphere.marathon.state.{MarathonStore, AppRepository, Timestamp}
import com.google.common.util.concurrent.AbstractExecutionThreadService
import javax.inject.{Named, Inject}
import java.util.{TimerTask, Timer}
import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicBoolean
import com.twitter.common.base.ExceptionalCommand
import com.twitter.common.zookeeper.Group.JoinException
import scala.Option
import com.twitter.common.zookeeper.Candidate
import com.twitter.common.zookeeper.Candidate.Leader
import scala.util.Random
import mesosphere.mesos.util.FrameworkIdUtil
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.util.InterruptibleFuture._

/**
 * Wrapper class for the scheduler
 *
 * @author Tobi Knaup
 */
class MarathonSchedulerService @Inject()(
    @Named(ModuleNames.NAMED_CANDIDATE) candidate: Option[Candidate],
    config: MarathonConf,
    @Named(ModuleNames.NAMED_LEADER_ATOMIC_BOOLEAN) leader: AtomicBoolean,
    appRepository: AppRepository,
    frameworkIdUtil: FrameworkIdUtil,
    scheduler: MarathonScheduler)
  extends AbstractExecutionThreadService with Leader {

  // TODO use a thread pool here
  import ExecutionContext.Implicits.global

  // Time to wait before trying to reconcile app tasks after driver starts
  val reconciliationInitialDelay =
    Duration(config.reconciliationInitialDelay(), MILLISECONDS)

  // Interval between task reconciliation operations
  val reconciliationFrequency =
    Duration(config.reconciliationFrequency(), MILLISECONDS)

  val reconciliationTimer = new Timer("reconciliationTimer")

  val log = Logger.getLogger(getClass.getName)

  val frameworkName = "marathon-" + Main.properties.getProperty("marathon.version")

  val frameworkInfo = FrameworkInfo.newBuilder()
    .setName(frameworkName)
    .setFailoverTimeout(config.mesosFailoverTimeout())
    .setUser(config.mesosUser())
    .setCheckpoint(config.checkpoint())


  // Set the framework ID
  frameworkIdUtil.fetch() match {
    case Some(id) => {
      log.info(s"Setting framework ID to ${id.getValue}")
      frameworkInfo.setId(id)
    }
    case None => {
      log.info("No previous framework ID found")
    }
  }
  // Set the role, if provided.
  config.mesosRole.get.map(frameworkInfo.setRole)

  val driver = new MesosSchedulerDriver(
    scheduler,
    frameworkInfo.build,
    config.mesosMaster()
  )

  var abdicateCmd: Option[ExceptionalCommand[JoinException]] = None

  def defaultWait = {
    appRepository.defaultWait
  }

  def startApp(app: AppDefinition): Future[_] = {
    // Backwards compatibility
    val oldPorts = app.ports
    val newPorts = oldPorts.map(p => if (p == 0) newAppPort(app) else p)

    if (oldPorts != newPorts) {
      val asMsg = Seq(oldPorts, newPorts).map("[" + _.mkString(", ") + "]")
      log.info(s"Assigned some ports for ${app.id}: ${asMsg.mkString(" -> ")}")
    }

    scheduler.startApp(driver, app.copy(ports = newPorts))
  }

  def stopApp(app: AppDefinition): Future[_] = {
    scheduler.stopApp(driver, app)
  }

  def updateApp(appName: String, appUpdate: AppUpdate): Future[_] =
    scheduler.updateApp(driver, appName, appUpdate).map { updatedApp =>
      scheduler.scale(driver, updatedApp)
    }

  def listApps(): Iterable[AppDefinition] = 
    Await.result(appRepository.apps, defaultWait)

  def listAppVersions(appName: String): Iterable[Timestamp] =
    Await.result(appRepository.listVersions(appName), defaultWait)

  def getApp(appName: String): Option[AppDefinition] = {
    Await.result(appRepository.currentVersion(appName), defaultWait)
  }

  def getApp(appName: String, version: Timestamp) : Option[AppDefinition] = {
    Await.result(appRepository.app(appName, version), defaultWait)
  }
  
  def killTasks(
    appName: String,
    tasks: Iterable[MarathonTask],
    scale: Boolean
  ): Iterable[MarathonTask] = {
    if (scale) {
      getApp(appName) foreach { app =>
        val appUpdate = AppUpdate(instances = Some(app.instances - tasks.size))
        Await.result(scheduler.updateApp(driver, appName, appUpdate), defaultWait)
      }
    }

    tasks.foreach { task =>
      log.info(f"Killing task ${task.getId} on host ${task.getHost}")
      driver.killTask(TaskID.newBuilder.setValue(task.getId).build)
    }

    tasks
  }

  //Begin Service interface
  def run() {
    log.info("Starting up")
    if (leader.get) {
      runDriver()
    } else {
      offerLeadership()
    }
  }

  override def triggerShutdown() {
    log.info("Shutting down")
    abdicateCmd.map{ case cmd =>
      // Await.result never interrupt its internal thread even if timeout occurred.
      // If the thread weren't interrupted, abdicateCmd will try to connect to ZK forever.
      val (execute, interrupt) = interruptibleFuture{ cmd.execute }
      try {
        // TODO(everpeace) make it configurable
        Await.result(execute , 10 seconds)
      } catch {
        case e: Throwable =>
          log.log(Level.SEVERE, "Problem happened in canceling zookeeper membership.", e)
          interrupt()
      }
    }
    stopDriver()
    reconciliationTimer.cancel
  }

  def runDriver() {
    log.info("Running driver")
    scheduleTaskReconciliation
    driver.run()
  }

  def stopDriver() {
    log.info("Stopping driver")
    driver.stop(true) // failover = true
  }

  def isLeader = {
    leader.get() || getLeader.isEmpty
  }

  def getLeader: Option[String] = {
    if (candidate.nonEmpty && candidate.get.getLeaderData.isPresent) {
      return Some(new String(candidate.get.getLeaderData.get))
    }
    None
  }
  //End Service interface

  //Begin Leader interface, which is required for CandidateImpl.
  def onDefeated() {
    log.info("Defeated")
    leader.set(false)
    stopDriver()

    // Don't offer leadership if we're shutting down
    if (isRunning) {
      offerLeadership()
    }
  }

  def onElected(abdicate: ExceptionalCommand[JoinException]) {
    log.info("Elected")
    abdicateCmd = Some(abdicate)
    leader.set(true)
    runDriver()
  }
  //End Leader interface

  private def scheduleTaskReconciliation {
    reconciliationTimer.schedule(
      new TimerTask { def run() { scheduler.reconcileTasks(driver) }},
      reconciliationInitialDelay.toMillis,
      reconciliationFrequency.toMillis
    )
  }

  private def offerLeadership() {
    if (candidate.nonEmpty) {
      log.info("Offering leadership.")
      candidate.get.offerLeadership(this)
    }
  }

  private def newAppPort(app: AppDefinition): Integer = {
    // TODO this is pretty expensive, find a better way
    val assignedPorts = listApps().map(_.ports).flatten.toSeq
    var port = 0
    do {
      port = config.localPortMin() + Random.nextInt(config.localPortMax() - config.localPortMin())
    } while (assignedPorts.contains(port))
    port
  }
}
