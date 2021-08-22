package com.igniteplus.data.pipeline.Cleanse

import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.cleanse.Cleanser.removeDuplicates
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.DataFrame

import org.scalatest.flatspec.AnyFlatSpec

class CleanseTest extends AnyFlatSpec with Helper{

  "removeDuplicates() method" should "remove the duplicates from the inputDF" in {
    val deDuplicatedFileTestDf : DataFrame = readFile(DEDUPLICATION_TEST_READ, FILE_FORMAT)(spark)
    val deDuplicatedDF : DataFrame = removeDuplicates(deDuplicatedFileTestDf,PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA,Some(ORDER_BY_COLUMN))
    val deDuplicatedCount : Long = deDuplicatedDF.count()
    val expectedCount : Long = 2
    assertResult(expectedCount)(deDuplicatedCount)
  }

}
