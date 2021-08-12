package com.igniteplus.data.pipeline

import com.igniteplus.data.pipeline.constants.ApplicationConstants
import com.igniteplus.data.pipeline.exception.{FileReadException, FileWriteException}
import com.igniteplus.data.pipeline.service.{FileReaderService, PipelineService}
import org.apache.spark.internal._
import com.sun.org.slf4j.internal.LoggerFactory
import com.sun.org.slf4j.internal


object DataPipeline extends Logging {

  def main(args : Array[String]) : Unit = {
    
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
  }
    
}
