package analyzer.expression

import org.apache.spark.sql.catalyst.expressions.{Add, AggregateWindowFunction, AttributeReference, Expression, GreaterThanOrEqual, If, Literal}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}


case class EventGroup(deltaTime: Expression) extends AggregateWindowFunction {
  self: Product =>
  def this() = this(Literal(0))

  override def children: Seq[Expression] = deltaTime :: Nil

  override def dataType: DataType = LongType

  protected val previousDelta = AttributeReference("previousDelta", IntegerType, nullable = false)()
  protected val currentGroup = AttributeReference("currentGroup", LongType, nullable = true)()

  override val aggBufferAttributes: Seq[AttributeReference] = currentGroup :: previousDelta :: Nil

  override val initialValues: Seq[Expression] = Literal(0L) :: Literal(0) :: Nil
  override val updateExpressions: Seq[Expression] =
    If( GreaterThanOrEqual(deltaTime, previousDelta), Add(currentGroup, Literal(1L)), currentGroup) :: deltaTime :: Nil

  override val evaluateExpression: Expression = aggBufferAttributes.head

  override def prettyName: String = "EventId"
}

