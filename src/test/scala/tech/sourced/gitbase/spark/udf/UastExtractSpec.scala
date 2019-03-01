package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{BinaryType, StructField}

class UastExtractSpec extends BaseUdfSpec {

  import spark.implicits._

  behavior of "UastExtract"

  it should "work as a registered UDF" in {
    val rolesDf = spark.sqlContext.sql("SELECT *, " + UastExtract.name +
      "(uast(blob_content, language(file_path,blob_content), '')," +
      " '@role') AS roles FROM " + BaseUdfSpec.filesName)

    rolesDf.schema.fields should contain(StructField("roles", BinaryType))
  }

  it should "work as an UDF in regular code" in {
    val rolesDf = filesDf.withColumn(
      "roles",
      UastExtract(
        UastMode(lit("annotated"), 'blob_content, Language('file_path, 'blob_content)),
        lit("@role")
      ))

    rolesDf.schema.fields should contain(StructField("roles", BinaryType))
  }

  // TODO add test again when bblfsh updates scala client to latest version
  it should "extract properties from UAST nodes" ignore {
    val keys = Seq(
      "@role",
      "@type",
      "@token",
      "@startpos",
      "@endpos",
      "foo"
    )

    keys.foreach(key => {
      val extractDf = filesDf.withColumn(
        key,
        UastExtract(
          UastMode(lit("annotated"), 'blob_content, Language('file_path, 'blob_content)),
          lit(key)
        ))

      extractDf.select('file_path, col(key)).collect().foreach(row => row.getString(0) match {
        case "src/foo.py" | "src/bar.java" | "foo" if Seq("@token", "foo").contains(key) =>
          new String(row.getAs[Array[Byte]](1)) should be("[]")
        case "src/foo.py" | "src/bar.java" | "foo" => row.getAs[Array[Byte]](1) should not be empty
        case _ => row.getAs[Seq[String]](1) should be(null)
      })

    })
  }
}
