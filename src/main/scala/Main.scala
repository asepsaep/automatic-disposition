import java.io.File

import akka.actor.{ ActorSystem, Props }
import akka.camel.{ CamelExtension, CamelMessage, Oneway, Producer }
import classifier.{ Classifier, ClassifierHub }
import com.typesafe.config.{ Config, ConfigFactory }
import models.{ BuildModelRequest, Ticket }
import org.apache.activemq.camel.component.ActiveMQComponent
import org.apache.spark.ml.tuning.CrossValidatorModel
import net.ceedubs.ficus.Ficus._
import org.apache.spark.SparkContext
import trainer.{ ModelBuilder, ModelBuilderHub }
import utils.SparkModule._

object Main extends App {

  implicit val system = ActorSystem("sistem-disposisi-otomatis")
  val camel = CamelExtension(system)
  val amqUrl = "nio://localhost:61616"
  camel.context.addComponent("activemq", ActiveMQComponent.activeMQComponent(amqUrl))
  System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", "*")

  val modelBuilderHub = system.actorOf(Props[ModelBuilderHub])
  val modelBuilder = system.actorOf(ModelBuilder.props(sparkContext, sparkSession, modelBuilderHub))

  val classifierHub = system.actorOf(Props[ClassifierHub])
  val classifier = system.actorOf(Classifier.props(sparkContext, sparkSession, classifierHub))

  val buildproducer = system.actorOf(Props[BuildProducer])
  val ticketSender = system.actorOf(Props[TicketSender])

  val config: Config = ConfigFactory.load()
  val modelPath = config.as[String]("model.path")

  Thread.sleep(1000)

  if (new File(modelPath).exists()) {
    val model = CrossValidatorModel.read.load(modelPath)
    modelBuilderHub ! CamelMessage(model.bestModel, Map(CamelMessage.MessageExchangeId → "NewModel"))
    Thread.sleep(1000)
  } else {
    buildproducer ! CamelMessage(BuildModelRequest(), Map(CamelMessage.MessageExchangeId → "Build"))
    Thread.sleep(60000)
  }

  val ticket = Ticket(
    id = Some(1),
    reporter = None,
    assignee = None,
    assigneeName = None,
    status = None,
    priority = None,
    title = None,
    description = Some("pungli sekolah"),
    resolution = None
  )

  ticketSender ! CamelMessage(ticket, Map(CamelMessage.MessageExchangeId → "NewTicket"))
  Thread.sleep(1000)

}

class BuildProducer extends Producer with Oneway {
  override def endpointUri: String = "activemq:topic:Build.Model"
}

class TicketSender extends Producer with Oneway {
  override def endpointUri: String = "activemq:topic:Classifier"
}