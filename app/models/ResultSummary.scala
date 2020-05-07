
package models

import java.time.{Duration, Instant}

case class ResultSummary(startAt: Instant, endAt: Instant, state: String, error: String) {
  val durationInSecond: Double = Duration.between(startAt, endAt).toMillis / 1000.0
}

object ResultSummary {
  val LatestNResults = 20
}

