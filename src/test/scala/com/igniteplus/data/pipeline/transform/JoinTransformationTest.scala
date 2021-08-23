package com.igniteplus.data.pipeline.transform

import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.transform.JoinTransformation.joinTable
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class JoinTransformationTest extends AnyFlatSpec with BeforeAndAfterAll with Helper {
  "join() method" should "perform left join of two dataframes" in {
    val clickstreamDf: DataFrame = readFile(INPUT_JOIN_CLICKSTREAM, FILE_FORMAT)
    val itemDf: DataFrame = readFile(INPUT_JOIN_ITEM, FILE_FORMAT)
    val jointDf: DataFrame = joinTable(clickstreamDf, itemDf,JOIN_KEY,JOIN_TYPE)
    val jointCount: Long = jointDf.count()
    val expectedCount: Long = 3
    assertResult(expectedCount)(jointCount)
    }
}
