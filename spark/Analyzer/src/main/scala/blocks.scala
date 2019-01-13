package analyzer

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
  birthTime: Double,
  deathTime: Double,
  position: Position,
  mutationId: Long,
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
    birthTime,
    deathTime,
    position,
    mutationId,
    mutation
  )
}

case class ChronicleLine(
  id: Long,
  parent_id: Long,
  birth_time: Double,
  death_time: Double,
  position_0: Float,
  position_1: Float,
  position_2: Float,
  mutation_id: Long,
  birth_efficiency: Float,
  birth_resistance: Float,
  lifespan_efficiency: Float,
  lifespan_resistance: Float,
  success_efficiency: Float,
  success_resistance: Float
  ){
  def position = Position(position_0, position_1, position_2);
  def mutation = Mutation( 
    birth_efficiency, birth_resistance,
    lifespan_efficiency, lifespan_resistance,
    success_efficiency, success_resistance);
  def toChronicleEntry = ChronicleEntry( 
    id, parent_id, 
    birth_time, death_time,
    position, 
    mutation_id, mutation);
  }



