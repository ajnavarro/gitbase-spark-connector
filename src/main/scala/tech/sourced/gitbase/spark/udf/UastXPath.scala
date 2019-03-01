package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.bblfsh.client.BblfshClient

object UastXPath extends CustomUDF {
  /** Name of the function. */
  override def name: String = "uast_xpath"

  /** Function to execute when this function is called. */
  override def function: UserDefinedFunction = udf(get _)

  def get(marshaledNodes: Array[Byte], query: String): Option[Array[Byte]] = {
    // TODO remove when bblfsh updates scala client to latest version
    throwUnsupportedException(name)

    val nodes = BblfshUtils.unmarshalNodes(marshaledNodes).getOrElse(Seq())
    val filtered = nodes.flatMap(BblfshClient.filter(_, query))
    BblfshUtils.marshalNodes(filtered)
  }
}
