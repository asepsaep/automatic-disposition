package utils

import com.typesafe.config.{ Config, ConfigFactory }
import net.ceedubs.ficus.Ficus._

object Preprocessor {

  val config: Config = ConfigFactory.load()
  val stopwords = config.as[Seq[String]]("stopwords")

  private def removeStopwords(text: String): String = {
    var output = text.split(' ')
    for (stopword ← stopwords) {
      output = output.filter(_ != stopword)
    }
    output.mkString(" ")
  }

  def clean(text: String): String = {
    val output = text.toLowerCase
      .replaceAll("\"", " ")
      .replaceAll("<br />", " ")
      .replaceAll("[^A-Za-z0-9 ]", " ")
      .replaceAll("[0-9]", " ")
      .replaceAll(" [a-z]{1} ", " ")
      .replaceAll(" +", " ")
      .trim()

    removeStopwords(output)
  }

}
