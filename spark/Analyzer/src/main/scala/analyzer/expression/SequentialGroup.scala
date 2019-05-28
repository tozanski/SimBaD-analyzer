package analyzer.expression

import org.apache.spark.sql.catalyst.expressions.{Add, AggregateWindowFunction, AttributeReference, Expression, If, Literal}
import org.apache.spark.sql.types.{DataType, LongType}


case class SequentialGroup(marker: Expression) extends AggregateWindowFunction {
  self: Product =>
  def this() = this(Literal(0))

  override def children: Seq[Expression] = marker :: Nil

  override def dataType: DataType = LongType

  protected val zero = Literal(0L)
  protected val one = Literal(1L)

  protected val currentGroup = AttributeReference("currentGroup", LongType, nullable = true)()

  override val aggBufferAttributes: Seq[AttributeReference] = currentGroup :: Nil

  override val initialValues: Seq[Expression] = zero :: Nil
  override val updateExpressions: Seq[Expression] = If(marker, Add(currentGroup, one), currentGroup) :: Nil

  override val evaluateExpression: Expression = aggBufferAttributes.head

  override def prettyName: String = "sequential_group"
}

