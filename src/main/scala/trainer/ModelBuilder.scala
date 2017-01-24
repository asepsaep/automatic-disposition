package trainer

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.camel.{ CamelMessage, Consumer, Oneway, Producer }
import com.typesafe.config.{ Config, ConfigFactory }
import model.{ BuildModelRequest, Ticket, TicketSummary }
import org.apache.spark.SparkContext
import org.apache.spark.ml.{ Model, Pipeline, Transformer }
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{ CrossValidator, ParamGridBuilder }
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import net.ceedubs.ficus.Ficus._

object ModelBuilder {
  def props(
    sparkContext: SparkContext,
    sparkSession: SparkSession,
    modelBuilderHub: ActorRef) = Props(new ModelBuilder(sparkContext, sparkSession, modelBuilderHub))
  case class BatchTrainerModel(model: Option[Transformer])
  case class Train(corpus: Dataset[Ticket])
}

class ModelBuilder(sparkContext: SparkContext, sparkSession: SparkSession, modelBuilderHub: ActorRef) extends Actor with Consumer {

  override def endpointUri: String = "activemq:topic:Build.Model"

  import ModelBuilder._

  val sqlContext = sparkSession.sqlContext
  import sqlContext.implicits._

  val config: Config = ConfigFactory.load()
  val modelPath = config.as[String]("model.path")

  override def receive = {
    case event: CamelMessage if event.headers(CamelMessage.MessageExchangeId) == "Build" ⇒
      val dataset = initializeDataset()
      val model = buildModel(dataset)
      modelBuilderHub ! CamelMessage(model, Map(CamelMessage.MessageExchangeId → "NewModel"))
  }

  protected def initializeDataset(): Dataset[TicketSummary] = {
    val dbUrl = config.as[String]("db.ticket.url")
    val dbTable = config.as[String]("db.ticket.table")
    val dbUser = config.as[String]("db.ticket.user")
    val dbPassword = config.as[String]("db.ticket.password")

    val opts = Map("url" → s"$dbUrl?user=$dbUser&password=$dbPassword", "dbtable" → dbTable, "driver" → "org.postgresql.Driver")
    val df = sqlContext.read.format("jdbc").options(opts).load()
    val dataset = df.map {
      case row ⇒ TicketSummary(row.getAs[String]("description"), row.getAs[String]("assignee"))
    }

    dataset

  }

  protected def buildModel(corpus: Dataset[TicketSummary]): Transformer = {
    val data = corpus.map(t ⇒ (t.description, t.assignee)).toDF("description", "assignee")

    val indexer = new StringIndexer().setInputCol("assignee").setOutputCol("label").fit(data)
    val tokenizer = new Tokenizer().setInputCol("description").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("raw_features")
    val idf = new IDF().setInputCol("raw_features").setOutputCol("features")
    val nb = new NaiveBayes()
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("prediction_label").setLabels(indexer.labels)

    val pipeline = new Pipeline().setStages(Array(indexer, tokenizer, hashingTF, idf, nb, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(20000))
      .addGrid(nb.smoothing, Array(1.0))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val result = cv.fit(data)
    result.write.overwrite().save(modelPath)

    val bestModel = result.bestModel
    bestModel

    /* Evaluation

    val predictionAndLabels = bestModel
      .transform(data)
      .select("prediction", "label")
      .map { case Row(prediction: Double, label: Double) ⇒ (prediction, label) }

    val metrics = new MulticlassMetrics(predictionAndLabels.rdd)

    println(metrics)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("assignee")
      .setPredictionCol("prediction_label")
      .setMetricName("accuracy")

    val prediction = bestModel
      .transform(data)
      .select("assignee", "prediction_label")
      .map { case Row(assignee: String, predictionLabel: String) ⇒ if (assignee == predictionLabel) 1 else 0 }
      .reduce(_ + _)

    val accuracy = (prediction.toDouble / data.count()) * 100

    println(accuracy)

    */
  }

}

class ModelBuilderHub extends Producer with Oneway {
  override def endpointUri: String = "activemq:topic:Classifier"
}