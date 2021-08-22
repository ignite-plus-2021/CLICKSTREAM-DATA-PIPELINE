package com.igniteplus.data.pipeline.Service

import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.constants.ApplicationConstants.SPARK_CONF
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class FileWriterServiceTest extends AnyFlatSpec with Helper{

  val spark = createSparkSession(SPARK_CONF)

  val testDf : DataFrame = readFile(writeTestCaseInputPath,fileFormat)(spark)
  val testDfCount:Long = testDf.count()


  "writeFile() method" should "write data to the given location" in {

    if(testDfCount!=0)
    {
      writeFile(testDf,fileFormat,writeTestCaseOutputPath)
      val readSampleOutputDf:DataFrame = readFile(writeTestCaseOutputPath,fileFormat)(spark)
      val checkOutputFile = readSampleOutputDf.count()
      assertResult(testDfCount)(checkOutputFile)
    }
  }

}
