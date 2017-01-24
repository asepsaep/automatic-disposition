package models

import java.time.OffsetDateTime

import utils.Preprocessor

case class Ticket(
  id: Option[Long],
  reporter: Option[String],
  assignee: Option[String],
  assigneeName: Option[String],
  createdAt: OffsetDateTime = OffsetDateTime.now(),
  closedAt: Option[OffsetDateTime] = None,
  status: Option[String],
  priority: Option[String],
  title: Option[String],
  description: Option[String],
  resolution: Option[String]) {

  def toTicketSummary = TicketSummary(Preprocessor.clean(description.getOrElse("")), assignee.getOrElse(""))

}

case class TicketSummary(
  description: String,
  assignee: String
)

case class TicketProbability(
  ticket: Ticket,
  probability: Double
)