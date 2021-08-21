package com.igniteplus.data.pipeline.Helper

trait Helper {

  /* Helpers for File Reader Service Test Case */
  val READ_LOCATION : String = "data/Test_Inputs/FileReaderServiceTestCaseInput.csv"
  val FILE_FORMAT : String = "csv"
  val COUNT_SHOULD_BE : Int = 4
  val READ_WRONG_LOCATION : String = "data/Test_Inputs/FileReaderServiceTestCaseInp.csv"

  /* Helpers for removeDuplicates Test Case*/
  val DEDUPLICATION_TEST_READ : String = "data/Test_Inputs/DeDuplicationTestCaseInput.csv"
  val PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA : Seq[String] = Seq("session_id","item_id")
  val ORDER_BY_COLUMN : String = "event_timestamp"

}
