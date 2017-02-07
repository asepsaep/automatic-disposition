package classifier

import java.time.OffsetDateTime

import akka.actor.{ Actor, ActorRef, Props }
import akka.actor.Actor.Receive
import akka.camel.{ CamelMessage, Consumer, Oneway, Producer }
import models.{ Ticket, TicketProbability }
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{ Row, SparkSession }
import utils.Preprocessor

object Classifier {
  def props(sparkContext: SparkContext, sparkSession: SparkSession, classifierHub: ActorRef) = Props(new Classifier(sparkContext, sparkSession, classifierHub))
}

class Classifier(sparkContext: SparkContext, sparkSession: SparkSession, classifierHub: ActorRef) extends Actor with Consumer {

  val sqlContext = sparkSession.sqlContext
  import sqlContext.implicits._

  var model: Option[Transformer] = None

  override def endpointUri: String = "activemq:topic:Classifier"

  override def receive = {
    case event: CamelMessage if event.headers(CamelMessage.MessageExchangeId) == "NewTicket" ⇒ {
      val ticket = event.bodyAs[Ticket]
      model.fold { TicketProbability(ticket, 0.0) } { classifier ⇒
        val testData = sqlContext.createDataFrame(Seq(ticket.toTicketSummary)).toDF()
        val result = classifier.transform(testData).select("description", "prediction_label", "probability", "prediction").collect().map {
          case Row(description: String, predictionLabel: String, probability: DenseVector, prediction: Double) ⇒
            TicketProbability(ticket.copy(assignee = Some(predictionLabel)), probability.values(prediction.toInt))
        }
        val updatedTicket = result(0)
        // println(OffsetDateTime.now())
        // println(updatedTicket)
        classifierHub ! CamelMessage(updatedTicket, Map(CamelMessage.MessageExchangeId → "NewTicketWithProbability"))
        updatedTicket
      }
    }

    case event: CamelMessage if event.headers(CamelMessage.MessageExchangeId) == "NewModel" ⇒ {
      model = Some(event.bodyAs[Transformer])
    }

  }

}

class ClassifierHub extends Producer with Oneway {
  override def endpointUri: String = "activemq:topic:Ticket.Receiver"
}