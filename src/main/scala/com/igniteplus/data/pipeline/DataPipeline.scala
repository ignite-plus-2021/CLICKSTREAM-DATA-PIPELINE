package com.igniteplus.data.pipeline

import com.igniteplus.data.pipeline.cleanseData.CleanData.filterRemoveNull
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{CLICKSTREAM_DATASET, COLUMNS_PRIMARY_KEY_CLICKSTREAM, COLUMNS_PRIMARY_KEY_ITEM, INPUT_NULL_CLICKSTREAM_DATA, INPUT_NULL_ITEM_DATA, ITEM_DATASET, READ_FORMAT}
import com.igniteplus.data.pipeline.service.FileReaderService
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataPipeline extends Logging {

  def main(args: Array[String]): Unit = {
    
    val logger : internal.Logger = LoggerFactory.getLogger(this.getClass)

    try {
            val pipeline = PipeLineService.executePipeLine()
       }

   catch {
     case ex: FileReadException =>
       logError("File read exception",ex)

     case ex: FileWriteException =>
       logError("file write exception", ex)

     case ex: Exception =>
         logError("Unknown exception",ex)

       sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
   }
  }
    
}
