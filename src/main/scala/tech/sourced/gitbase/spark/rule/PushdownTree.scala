package tech.sourced.gitbase.spark.rule

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{
  And,
  AttributeReference,
  Expression,
  Literal,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{StructField, StructType}
import tech.sourced.gitbase.spark._

object PushdownTree extends Rule[LogicalPlan] {

  /** @inheritdoc*/
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case logical.Project(Seq(), child) => child

    case n@logical.Project(
    list,
    r@DataSourceV2Relation(_, DefaultReader(servers, _, query))) =>
      if (containsGroupBy(query)) {
        r
      } else if (!canBeHandled(list) || containsDuplicates(list)) {
        fixAttributeReferences(n)
      } else {
        val newSchema = StructType(
          list.map(e => StructField(e.name, e.dataType, e.nullable, e.metadata))
        )
        val newOutput = list.map(e =>
          AttributeReference(e.name, e.dataType, e.nullable, e.metadata)()
        )
        DataSourceV2Relation(
          newOutput,
          DefaultReader(
            servers,
            newSchema,
            Project(list, query)
          )
        )
      }

    case n@logical.Filter(
    expr,
    DataSourceV2Relation(out, DefaultReader(servers, schema, query))) =>
      if (!canBeHandled(Seq(expr))) {
        fixAttributeReferences(n)
      } else {
        DataSourceV2Relation(
          out,
          DefaultReader(
            servers,
            schema,
            Filter(splitExpressions(expr), query)
          )
        )
      }

    // We should only push down local sorts.
    case n@logical.Sort(
    order,
    false,
    DataSourceV2Relation(out, DefaultReader(servers, schema, query))) =>
      if (!canBeHandled(order.map(_.child))) {
        fixAttributeReferences(n)
      } else {
        DataSourceV2Relation(
          out,
          DefaultReader(
            servers,
            schema,
            Sort(order, query)
          )
        )
      }

    case logical.LocalLimit(
    limit: Literal,
    DataSourceV2Relation(out, DefaultReader(servers, schema, query))) =>
      val limitNumber = limit.value match {
        case n: Int => n.toLong
        case n: Long => n
        case _ => throw new SparkException("limit literal should be a number")
      }

      DataSourceV2Relation(
        out,
        DefaultReader(
          servers,
          schema,
          Limit(limitNumber, query)
        )
      )

    case node: DataSourceV2Relation => node

    case node => fixAttributeReferences(node)
  }

  private def splitExpressions(expression: Expression): Seq[Expression] = {
    expression match {
      case And(left, right) =>
        splitExpressions(left) ++ splitExpressions(right)
      case e => Seq(e)
    }
  }

  private def canBeHandled(exprs: Seq[Expression]): Boolean = {
    exprs.flatMap(x => QueryBuilder.compileExpression(x)).length == exprs.length
  }

  private def containsDuplicates(exprs: Seq[NamedExpression]): Boolean = {
    exprs.groupBy(_.name).values.map(_.length).exists(_ > 1)
  }

}
