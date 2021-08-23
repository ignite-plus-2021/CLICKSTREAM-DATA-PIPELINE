package com.igniteplus.data.pipeline

import com.igniteplus.data.pipeline.constants.ApplicationConstants
import com.igniteplus.data.pipeline.constants.ApplicationConstants.SPARK_CONF
import com.igniteplus.data.pipeline.exception.{DqDuplicateCheckException, DqNullCheckException, FileReadException, FileWriteException}
import com.igniteplus.data.pipeline.service.{DqCheckService, PipelineService}
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.internal._
import com.sun.org.slf4j.internal.LoggerFactory
import com.sun.org.slf4j.internal


object DataPipeline extends Logging {

  def main(args : Array[String]) : Unit = {

    //IMPLICIT VALUE OF SPARK
    implicit val spark = createSparkSession(SPARK_CONF)

    val logger : internal.Logger = LoggerFactory.getLogger(this.getClass)

    try {
      PipelineService.executePipeline()
    }

    catch {
      case ex : FileReadException =>
        logError("File read exception",ex)
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)

      case ex: FileWriteException =>
        logError("file write exception", ex)
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)

      case ex: Exception =>
        logError("Unknown exception",ex)
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
    }

    try{
      DqCheckService.executeDqCheck()
    }

    catch{
      case e:DqNullCheckException => {
        logError("DQ check failed",e)
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
      }
      case e:DqDuplicateCheckException => {
        logError("DQ check failed",e)
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
      }
    }

  }

}