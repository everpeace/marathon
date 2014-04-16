package mesosphere.marathon

import mesosphere.chaos.App
import org.rogach.scallop.ScallopConf
import mesosphere.chaos.http.{HttpService, HttpModule, HttpConf}
import mesosphere.chaos.metrics.MetricsModule
import mesosphere.marathon.api.MarathonRestModule
import mesosphere.chaos.AppConfiguration
import mesosphere.marathon.event.{EventModule, EventConfiguration}
import mesosphere.marathon.event.http.{HttpEventModule, HttpEventConfiguration}
import com.google.inject.AbstractModule
import java.util.logging.{Level, Logger}
import java.util.Properties

/**
 * @author Tobi Knaup
 */
object Main extends App {

  val properties = new Properties
  properties.load(getClass.getClassLoader.getResourceAsStream("marathon.properties"))

  val log = Logger.getLogger(getClass.getName)
  log.info(s"Starting Marathon ${properties.getProperty("marathon.version")}")

  def modules() = {
    Seq(
      new HttpModule(conf) {
        // burst browser cache for assets
        protected override val resourceCacheControlHeader = Some("max-age=0, must-revalidate")
      },
      new MetricsModule,
      new MarathonModule(conf),
      new MarathonRestModule,
      new EventModule(conf)
    ) ++ getEventsModule()
  }

  //TODO(*): Rethink how this could be done.
  def getEventsModule(): Option[AbstractModule] = {
    if (conf.eventSubscriber.isSupplied) {
      conf.eventSubscriber() match {
        case "http_callback" => {
          log.warning("Using HttpCallbackEventSubscriber for event" +
            "notification")
          return Some(new HttpEventModule())
        }
        case _ => {
          log.warning("Event notification disabled.")
        }
      }
    }

    None
  }

  //TOOD(FL): Make Events optional / wire up.
  lazy val conf = new ScallopConf(args)
    with HttpConf with MarathonConf with AppConfiguration
      with EventConfiguration with HttpEventConfiguration with ZookeeperConf

  try{
    run(
      classOf[HttpService],
      classOf[MarathonSchedulerService]
    )
  }catch{
    case t: Throwable =>
      log.log(Level.SEVERE, "Marathon cannot be started.", t)
      sys.exit(9)
  }
}
