package classifier

import akka.actor.{ Actor, Props }
import akka.actor.Actor.Receive
import akka.camel.{ CamelMessage, Consumer }
import model.{ Ticket, TicketProbability }
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{ Row, SparkSession }
import util.Preprocessor

object Classifier {
  def props(sparkContext: SparkContext, sparkSession: SparkSession) = Props(new Classifier(sparkContext, sparkSession))
}

class Classifier(sparkContext: SparkContext, sparkSession: SparkSession) extends Actor with Consumer {

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
        println(result(0))
        result(0)
      }
    }

    case event: CamelMessage if event.headers(CamelMessage.MessageExchangeId) == "NewModel" ⇒ {
      model = Some(event.bodyAs[Transformer])
    }

  }

}
