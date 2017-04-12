package trainer

import java.time.LocalTime

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.camel.{ CamelMessage, Consumer, Oneway, Producer }
import com.typesafe.config.{ Config, ConfigFactory }
import models.{ BuildLog, BuildModelRequest, Ticket, TicketSummary }
import org.apache.spark.SparkContext
import org.apache.spark.ml.{ Model, Pipeline, Transformer }
import org.apache.spark.ml.classification._
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
    modelBuilderHub: ActorRef,
    buildLogger: ActorRef) = Props(new ModelBuilder(sparkContext, sparkSession, modelBuilderHub, buildLogger))
  case class BatchTrainerModel(model: Option[Transformer])
  case class Train(corpus: Dataset[Ticket])
  case class BuildMode(manual: Boolean, id: Option[Long])
}

class ModelBuilder(
  sparkContext: SparkContext,
  sparkSession: SparkSession,
  modelBuilderHub: ActorRef,
  buildLogger: ActorRef
) extends Actor with Consumer with ActorLogging {

  override def endpointUri: String = "activemq:topic:Build.Model"

  import ModelBuilder._

  val sqlContext = sparkSession.sqlContext
  import sqlContext.implicits._

  val config: Config = ConfigFactory.load()
  val modelPath = config.as[String]("model.basePath")

  var algorithm = "naive-bayes"

  override def receive = {
    case event: CamelMessage if event.headers(CamelMessage.MessageExchangeId) == "Algorithm" ⇒ {
      println("Receive new algoritm request")
      val newAlgorithm = event.bodyAs[String]
      algorithm = newAlgorithm
    }

    case event: CamelMessage if event.headers(CamelMessage.MessageExchangeId) == "Build" ⇒ {
      println("\n" + LocalTime.now)
      println("[ModelBuilder] Received Event Build Model Request")
      val dataset = initializeDataset(BuildMode(manual = false, id = None))
      val classifierAlgorithm: Classifier[_, _, _] = algorithm match {
        case "naive-bayes"   ⇒ new NaiveBayes()
        case "decision-tree" ⇒ new DecisionTreeClassifier()
        case "random-forest" ⇒ new RandomForestClassifier()
        case "one-vs-rest"   ⇒ new LogisticRegression().setMaxIter(10).setTol(1E-6).setFitIntercept(true)
      }
      val model = buildModel(dataset, BuildMode(manual = false, id = None), classifierAlgorithm)
      println("[ModelBuilt Event] ModelBuilder -> Classifier")
      modelBuilderHub ! CamelMessage(model, Map(CamelMessage.MessageExchangeId → "NewModel"))
    }

    case event: CamelMessage if event.headers(CamelMessage.MessageExchangeId) == "ManualBuildModel" ⇒ {
      println("\n" + LocalTime.now)
      println("[ModelBuilder] Received Event Build Model Request from User")
      val buildSessionId = event.bodyAs[Long]
      val dataset = initializeDataset(BuildMode(manual = true, id = Option(buildSessionId)))
      val classifierAlgorithm: ProbabilisticClassifier[_, _, _] = algorithm match {
        case "naive-bayes"   ⇒ new NaiveBayes()
        case "decision-tree" ⇒ new DecisionTreeClassifier()
        case "random-forest" ⇒ new RandomForestClassifier()
        case "one-vs-rest"   ⇒ new LogisticRegression().setMaxIter(10).setTol(1E-6).setFitIntercept(true)
      }
      val model = buildModel(dataset, BuildMode(manual = true, id = Option(buildSessionId)), classifierAlgorithm)
      println("[ModelBuilt Event] ModelBuilder -> Classifier")
      modelBuilderHub ! CamelMessage(model, Map(CamelMessage.MessageExchangeId → "NewModel"))
    }

  }

  protected def initializeDataset(buildMode: BuildMode): Dataset[TicketSummary] = {

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Initializing database..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    }
    val dbUrl = config.as[String]("db.ticket.url")
    val dbTable = config.as[String]("db.ticket.table")
    val dbUser = config.as[String]("db.ticket.user")
    val dbPassword = config.as[String]("db.ticket.password")

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Reading from table..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    }
    val opts = Map("url" → s"$dbUrl?user=$dbUser&password=$dbPassword", "dbtable" → dbTable, "driver" → "org.postgresql.Driver")
    val df = sqlContext.read.format("jdbc").options(opts).load()

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Collecting dataset..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    }
    val dataset = df.map {
      case row ⇒ TicketSummary(row.getAs[String]("description"), row.getAs[String]("assignee"))
    }

    dataset
  }

  protected def buildModel(corpus: Dataset[TicketSummary], buildMode: BuildMode, classifierAlgorithm: Classifier[_, _, _] = new NaiveBayes()): Transformer = {

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Loading dataset into Spark's DataFrame..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    } else log.info("Loading dataset into Spark's DataFrame...")
    val data = corpus.map(t ⇒ (t.description, t.assignee)).toDF("description", "assignee")

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Starting feature extraction..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    } else log.info("Starting feature extraction...")
    val indexer = new StringIndexer().setInputCol("assignee").setOutputCol("label").fit(data)
    val tokenizer = new Tokenizer().setInputCol("description").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("raw_features")
    val idf = new IDF().setInputCol("raw_features").setOutputCol("features")
    //    val nb = new NaiveBayes()
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("prediction_label").setLabels(indexer.labels)

    val pipeline = if (algorithm == "one-vs-rest")
      new Pipeline().setStages(Array(indexer, tokenizer, hashingTF, idf, new OneVsRest().setClassifier(classifierAlgorithm), labelConverter))
    else
      new Pipeline().setStages(Array(indexer, tokenizer, hashingTF, idf, classifierAlgorithm, labelConverter))

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Building model..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    } else log.info("Building model...")
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10000))
      .build()
    //      .addGrid(classifierAlgorithm.smoothing, Array(1.0))

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Starting cross validation..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    } else log.info("Starting cross validation...")
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Storing model into disk..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    } else log.info("Storing model into disk...")
    val result = cv.fit(data)
    result.write.overwrite().save(modelPath + "/" + algorithm)

    val bestModel = result.bestModel

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "Starting post-built analytics..."),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    } else log.info("Starting post-built analytics...")

    val predictionAndLabels = bestModel
      .transform(data)
      .select("prediction", "label")
      .map { case Row(prediction: Double, label: Double) ⇒ (prediction, label) }

    val metrics = new MulticlassMetrics(predictionAndLabels.rdd)

    val metricsInfo =
      s"""
         | weightedFalsePositiveRate: ${metrics.weightedFalsePositiveRate}
         | weightedTruePositiveRate: ${metrics.weightedTruePositiveRate}
         | weightedFMeasure: ${metrics.weightedFMeasure}
         | weightedPrecision: ${metrics.weightedPrecision}
         | weightedRecall: ${metrics.weightedRecall}
         | accuracy: ${metrics.accuracy}
       """.stripMargin

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), metricsInfo),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    } else log.info(metricsInfo)

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

    if (buildMode.manual) {
      buildLogger ! CamelMessage(
        BuildLog(buildMode.id.getOrElse(10000), "<br>Accuracy: " + accuracy + "<br><br>Done"),
        Map(CamelMessage.MessageExchangeId → "BuildLog"))
    } else log.info("Accuracy: " + accuracy)

    bestModel
  }

}

class ModelBuilderHub extends Producer with Oneway {
  override def endpointUri: String = "activemq:topic:Classifier"
}

class BuildLogger extends Producer with Oneway {
  override def endpointUri: String = "activemq:topic:Build.Log"
}