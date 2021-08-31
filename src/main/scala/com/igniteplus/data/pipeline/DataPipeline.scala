package com.igniteplus.data.pipeline

import com.igniteplus.data.pipeline.constants.ApplicationConstants
import com.igniteplus.data.pipeline.constants.ApplicationConstants.SPARK_CONF
import com.igniteplus.data.pipeline.exception.{DqDuplicateCheckException, DqNullCheckException, FileReadException, FileWriteException}
import com.igniteplus.data.pipeline.service.{DqCheckService, PipelineService}
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.internal.Logging


object DataPipeline extends Logging {

  var exitCode: Int = ApplicationConstants.FAILURE_EXIT_CODE

  def main(args : Array[String]) : Unit = {

    //IMPLICIT VALUE OF SPARK
    implicit val spark = createSparkSession(SPARK_CONF)

    try {
      PipelineService.executePipeline()
      DqCheckService.executeDqCheck()
      exitCode = ApplicationConstants.SUCCESS_EXIT_CODE
    }

    catch {
      case ex : FileReadException =>
        logError("File read exception",ex)

      case ex: FileWriteException =>
        logError("file write exception", ex)

      case ex:DqNullCheckException =>
        logError("DQ check failed",ex)

      case ex:DqDuplicateCheckException =>
        logError("DQ check failed",ex)

      case ex: Exception =>
        logError("Unknown exception",ex)
    }

   finally {
     logInfo(s"Pipeline completed with status $exitCode")
     spark.stop()
     sys.exit(exitCode)
   }

  }

}