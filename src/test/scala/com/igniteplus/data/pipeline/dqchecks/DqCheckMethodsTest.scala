package com.igniteplus.data.pipeline.dqchecks

import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.dqchecks.DqCheckMethods.{DqDuplicateCheck, DqNullCheck}
import com.igniteplus.data.pipeline.exception.{DqDuplicateCheckException, DqNullCheckException}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class DqCheckMethodsTest extends AnyFlatSpec with Helper{
  "DqNullCheck() method" should "throw exception if key columns have null values" in {
    val sampleDF: DataFrame = readFile(DQ_NULL_CHECK_INPUT, fileFormat)
    assertThrows[DqNullCheckException] {
      DqNullCheck(sampleDF,PRIMARY_KEY_COLUMNS)
    }
  }

  "DqDuplicateCheck() method" should "throw exception if key columns have duplicate values" in {
    val sampleDF: DataFrame = readFile(DQ_DUPLICATE_CHECK_INPUT, fileFormat)
    assertThrows[DqDuplicateCheckException] {
      DqDuplicateCheck(sampleDF,PRIMARY_KEY_COLUMNS,ORDER_BY_COL)
    }
  }


}
