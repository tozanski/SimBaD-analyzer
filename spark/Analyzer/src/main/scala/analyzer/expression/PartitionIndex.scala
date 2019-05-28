package analyzer.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Nondeterministic}
import org.apache.spark.sql.types.{DataType, LongType}

case class PartitionIndex() extends LeafExpression with Nondeterministic {

  @transient private[this] var partitionId: Long = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    partitionId = partitionIndex.toLong
  }

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override protected def evalInternal(input: InternalRow): Long = {
    partitionId
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val partitionIdTerm = "partitionId"
    ctx.addImmutableStateIfNotExists(CodeGenerator.JAVA_LONG, partitionIdTerm)
    ctx.addPartitionInitializationStatement(s"$partitionIdTerm = (long) partitionIndex;")

    ev.copy(code = code"""final ${CodeGenerator.javaType(dataType)} ${ev.value} = $partitionIdTerm;""",
      isNull = FalseLiteral)
  }

  override def prettyName: String = "partition_id"

  override def sql: String = s"$prettyName()"

}
