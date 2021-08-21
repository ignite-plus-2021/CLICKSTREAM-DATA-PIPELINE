package com.igniteplus.data.pipeline.transformation

import com.igniteplus.data.pipeline.Helper.Helper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import com.igniteplus.data.pipeline.transformation.Transform._
import com.igniteplus.data.pipeline.service.FileReaderService._

class TransformTest extends AnyFlatSpec with BeforeAndAfterAll with Helper {
  "join() method" should "perform left join of two dataframes" in {
    val clickstremDf: DataFrame = readFile(INPUT_JOIN_CLICKSTREAM, FILE_FORMAT)
    val itemDf: DataFrame = readFile(INPUT_JOIN_ITEM, FILE_FORMAT)
    val jointDf: DataFrame = join(clickstremDf, itemDf)
    val jointCount: Long = jointDf.count()
    val expectedCount: Long = 3
    assertResult(expectedCount)(jointCount)
  }
}
