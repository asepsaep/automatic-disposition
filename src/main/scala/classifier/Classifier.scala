package classifier

import akka.actor.{ Actor, Props }
import akka.actor.Actor.Receive
import akka.camel.{ CamelMessage, Consumer }
import model.Ticket
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{ Row, SparkSession }

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
      model.fold { println(ticket); ticket } { classifier ⇒
        val testData = sqlContext.createDataFrame(Seq(ticket)).toDF()
        val result = classifier.transform(testData).select("description", "prediction_label").collect.map {
          case Row(description: String, predictionLabel: String) ⇒ ticket.copy(assignee = predictionLabel)
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
