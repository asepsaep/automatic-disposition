package sample.camel

import akka.actor.{ ActorSystem, Props }
import akka.camel.{ CamelExtension, CamelMessage, Consumer, Oneway, Producer }
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.camel.component.ActiveMQComponent
import org.apache.activemq.ScheduledMessage.AMQ_SCHEDULED_DELAY
import org.apache.camel.builder.RouteBuilder

object ActiveMQExample extends App {

  implicit val system = ActorSystem("activemq-example")
  val camel = CamelExtension(system)
  val amqUrl = "nio://localhost:61616"
  camel.context.addComponent("activemq", ActiveMQComponent.activeMQComponent(amqUrl))

  System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", "*")

  val producer = system.actorOf(Props[SimpleProducer])
  val mainConsumer = system.actorOf(Props[MainConsumer])
  val anotherConsumer = system.actorOf(Props[AnotherConsumer])
  val miscConsumer = system.actorOf(Props[MiscConsumer])

  Thread.sleep(1000) // wait for setup

  producer ! Message("first")
  producer ! Message("second")
  producer ! CamelMessage(Message("third"), Map(AMQ_SCHEDULED_DELAY → 3000))

  Thread.sleep(5000) // wait for messages
  system.terminate()

}

case class Message(body: String)

class SimpleProducer extends Producer with Oneway {
  override def endpointUri: String = "activemq:topic:Hello.Dangdut"
}

class MainConsumer extends Consumer {
  override def endpointUri: String = "activemq:topic:Hello.Dangdut"

  override def receive: Receive = {
    case event: CamelMessage ⇒ println("[MainConsumer] %s" format event)
  }
}

class AnotherConsumer extends Consumer {
  override def endpointUri: String = "activemq:topic:Hello.Dangdut"

  override def receive: Receive = {
    case event: CamelMessage ⇒ println("[AnotherConsumer] %s" format event)
  }
}

class MiscConsumer extends Consumer {
  override def endpointUri: String = "activemq:topic:Hello.Dangdut"

  override def receive: Receive = {
    case event: CamelMessage ⇒
      println("[MiscConsumer] %s" format event.bodyAs[Message].body)
      println("[MiscConsumer] %s" format event)
  }
}