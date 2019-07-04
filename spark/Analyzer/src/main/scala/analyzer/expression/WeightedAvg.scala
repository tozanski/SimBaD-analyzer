package analyzer.expression

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
case class WeightedAvg(childValue: Expression, childCount: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = childValue :: childCount:: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes = FloatType::LongType::Nil

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(childValue.dataType, "function sum")

  private lazy val resultType = DoubleType

  private lazy val sumDataType = resultType
  private lazy val countDataType = LongType

  private lazy val sum = AttributeReference("sum", sumDataType)()
  private lazy val count = AttributeReference("count", countDataType)()

  private lazy val zeroSum = Cast(Literal(0), sumDataType)
  private lazy val zeroCount = Cast(Literal(0), countDataType)

  override lazy val aggBufferAttributes = sum :: count :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* sum = */ Literal.create(null, sumDataType),
    /* count = */ Literal.create(null, countDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    val partialCount = if (childCount.nullable) coalesce(childCount, zeroCount) else childCount

    val updatedSum =
      if (childValue.nullable)
        coalesce(coalesce(sum, zeroSum) + Multiply(childValue.cast(sumDataType), partialCount), sum)
      else
        coalesce(sum, zeroSum) + Multiply(childValue.cast(sumDataType), partialCount)

    val updatedCount =
      if (childCount.nullable)
        coalesce(coalesce(count, zeroCount) + childCount.cast(countDataType), count)
      else
        coalesce(count, zeroCount) + childCount.cast(countDataType)

    updatedSum :: updatedCount :: Nil
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      coalesce(coalesce(sum.left, zeroSum) + sum.right, sum.left),
      /* count = */
      coalesce(coalesce(count.left, zeroCount) + count.right, count.left)
    )
  }

  override lazy val evaluateExpression: Expression = Divide(sum,count)
}
