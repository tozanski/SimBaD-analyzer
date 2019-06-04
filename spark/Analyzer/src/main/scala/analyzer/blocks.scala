package analyzer

import scala.annotation.switch

case class Position(
  x: Float,
  y: Float,
  z: Float
)

case class Mutation(
  birthEfficiency: Float,
  birthResistance: Float,
  lifespanEfficiency: Float,
  lifespanResistance: Float,
  successEfficiency: Float,
  successResistance: Float
)

case class Cell(
  position: Position,
  mutationId: Long,
  mutation: Mutation
)

case class EventKind(encoded: Int){
  def this(str: String) = {
    this( str match {
      case "NONE"         => 0
      case "CREATED"      => 1
      case "REMOVED"      => 2
      case "MODIFIED"     => 3
      case "TRANSFORMED"  => 4
      case "JUPED_OUT"    => 5
      case "JUMPED_IN"    => 6

      case _ => -1
    })
  }

  override def toString(): String = { 
    (encoded: @switch) match {
      case 0 => "NONE"
      case 1 => "CREATED"
      case 2 => "REMOVED"
      case 3 => "MODIFIED"
      case 4 => "TRANSFORMED"
      case 5 => "JUMPED_OUT"
      case 6 => "JUMPED_IN"

      case _ => "invalid event kind"
    }
  }
}

case class Event(
  time: Double,
  timeDelta: Int,
  eventKind: EventKind,
  position: Position,
  mutationId: Long,
  mutation: Mutation
)

case class EnumeratedEvent(
  timeOrder: Long,
  time: Double,
  timeDelta: Int,
  eventKind: EventKind,
  position: Position,
  mutationId: Long,
  mutation: Mutation
)

case class GroupedEvent(
  time: Double,
  timeDelta: Int,
  eventId: Long,
  eventKind: EventKind,
  position: Position,
  mutation: Mutation
)

case class ChronicleEntry(
  particleId: Long,
  parentId: Long,
  birthTime: Double,
  deathTime: Double,
  position: Position,
  mutationId: Long,
  mutation: Mutation
){
  def toCell = Cell(
    position,
    mutationId,
    mutation
  )
}

case class Clone(
  mutationId: Long,
  count: Long,
  birthEfficiency: Float,
  birthResistance: Float,
  lifespanEfficiency: Float,
  lifespanResistance: Float,
  successEfficiency: Float,
  successResistance: Float
)

case class MutationTreeLink(mutationId: Long, parentMutationId: Long)
case class Ancestry(mutationId: Long, ancestors: Array[Long])
