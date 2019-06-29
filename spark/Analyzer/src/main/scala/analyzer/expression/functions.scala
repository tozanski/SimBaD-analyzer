package analyzer.expression

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, MonotonicallyIncreasingID}

object functions {
  private def withExpr(expr: Expression): Column = new Column(expr) //copy & paste
  private def withAggregateFunction(func: AggregateFunction, isDistinct: Boolean = false): Column = {
    new Column(func.toAggregateExpression(isDistinct))
  }
  //
  def eventGroup(marker: Column): Column = withExpr{ EventGroup(marker.expr) }
  def weightedAvg(value: Column, count: Column): Column = withAggregateFunction { WeightedAvg(value.expr, count.expr) }
  def weightedStdDev(value: Column, count: Column): Column = withAggregateFunction { WeightedStdDev(value.expr, count.expr) }
}
