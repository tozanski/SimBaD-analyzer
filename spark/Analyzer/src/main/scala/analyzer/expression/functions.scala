package analyzer.expression

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, MonotonicallyIncreasingID}

object functions {
  private def withExpr(expr: Expression): Column = new Column(expr) //copy & paste
  //
  def sequentialGroup(marker: Column): Column = withExpr { SequentialGroup(marker.expr) }
  def partition_id(): Column = withExpr { PartitionIndex() }
}
