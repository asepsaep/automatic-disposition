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

  override def toString =
    s"""
       | id = ${id.getOrElse("")}
       | reporter = ${reporter.getOrElse("")}
       | assignee = ${assignee.getOrElse("")}
       | assigneeName = ${assigneeName.getOrElse("")}
       | createdAt = ${createdAt}
       | closedAt = ${closedAt.getOrElse("")}
       | status = ${status.getOrElse("")}
       | priority = ${priority.getOrElse("")}
       | title = ${title.getOrElse("")}
       | description= ${description.getOrElse("")}
       | resolution = ${resolution.getOrElse("")}
    """.stripMargin

}

case class TicketSummary(
  description: String,
  assignee: String
)

case class TicketProbability(
  ticket: Ticket,
  probability: Double
)