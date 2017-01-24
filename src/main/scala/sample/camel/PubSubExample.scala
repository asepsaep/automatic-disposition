package sample.camel

import akka.actor.Actor.Receive
import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.camel.{ CamelExtension, Consumer, Producer }
import org.apache.camel.builder.RouteBuilder

object PubSubExample {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("pubsub-example")
    val ep = system.actorOf(Props[EventProducer])
    val trigger = system.actorOf(Props(classOf[Trigger], ep))
    val ec1 = system.actorOf(Props[EventConsumer1])
    val ec2 = system.actorOf(Props[EventConsumer2])
    val ec3 = system.actorOf(Props[EventConsumer3])
    CamelExtension(system).context.addRoutes(new CustomRouteBuilder)
  }

  class EventProducer extends Actor with Producer {
    override def endpointUri: String = "direct:ep"

    override def routeResponse(msg: Any) { sender() forward msg }
  }

  class Trigger(producer: ActorRef) extends Actor with Consumer {
    override def endpointUri: String = "jetty:http://0.0.0.0:10000/"
    override def receive: Receive = {
      case event ⇒ producer forward event
    }

  }

  class EventConsumer1 extends Actor with Consumer {
    override def endpointUri: String = "direct:ec1"
    override def receive: Receive = {
      case event ⇒ println("[EC1]%s" format event)
    }
  }

  class EventConsumer2 extends Actor with Consumer {
    override def endpointUri: String = "direct:ec2"
    override def receive: Receive = {
      case event ⇒ println("[EC2]%s" format event)
    }
  }

  class EventConsumer3 extends Actor with Consumer {
    override def endpointUri: String = "direct:ec3"
    override def receive: Receive = {
      case event ⇒ println("[EC3]%s" format event)
    }
  }

  class CustomRouteBuilder extends RouteBuilder {
    override def configure(): Unit = {
      from("direct:ep").multicast().to("direct:ec1", "direct:ec2", "direct:ec3")
    }
  }

}
