package analyzer.expression

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

case class WeightedStdDev(childValue: Expression, childCount: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = childValue :: childCount:: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes = FloatType :: LongType :: Nil

  //override def checkInputDataTypes(): TypeCheckResult =
  //  TypeUtils.checkForNumericExpr(childValue.dataType, "function sum")

  private lazy val resultType = DoubleType

  private lazy val avgDataType = resultType
  private lazy val countDataType = LongType
  private lazy val varDataType = resultType

  private lazy val avg = AttributeReference("avg", avgDataType, nullable = false)()
  private lazy val count = AttributeReference("count", countDataType, nullable = false)()
  private lazy val vari = AttributeReference("vari", varDataType, nullable = false)()

  private lazy val zeroAvg = Cast(Literal(0), avgDataType)
  private lazy val zeroCount = Cast(Literal(0), countDataType)
  private lazy val zeroVar = Cast(Literal(0), varDataType)

  override lazy val aggBufferAttributes = avg :: count :: vari :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(0.0, avgDataType),
    Literal.create(0, countDataType),
    Literal.create(0.0, varDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    val incValue = if (childValue.nullable) coalesce(Cast(childValue, avgDataType), zeroAvg) else  Cast(childValue, avgDataType)
    val incCount = if (childCount.nullable) coalesce(childCount, zeroCount) else childCount

    val newCount = count + incCount
    val delta = incValue - avg
    val newAvg = avg + delta * Cast(incCount / newCount, avgDataType)
    val newVar = vari + delta*delta * count* Cast(incCount / newCount, avgDataType)

    newAvg :: newCount :: newVar :: Nil
    //avg::count::vari::Nil
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    val newCount = count.left + count.right
    val delta = avg.right - avg.left
    val deltaN = If(newCount === 0, 0.0, delta/Cast(newCount, avgDataType))
    val newAvg = avg.left + deltaN * Cast(count.right, avgDataType)
    val newVari = vari.left + vari.right + delta * deltaN * Cast(count.left * count.right, avgDataType)

    newAvg :: newCount :: newVari :: Nil

    //avg::count::vari::Nil
  }

  override lazy val evaluateExpression: Expression = {

    If(count === 0, Literal.create(null, DoubleType),
      If(count === 1, Double.NaN, sqrt(vari / Cast(count - 1, varDataType))))
    //Literal.create(null, DoubleType)
  }
}
