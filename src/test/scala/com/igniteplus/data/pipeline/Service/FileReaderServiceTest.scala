package com.igniteplus.data.pipeline.Service
import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.exception.FileReadException
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class FileReaderServiceTest extends AnyFlatSpec with BeforeAndAfterAll with Helper {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Tests").master("local").getOrCreate()
  }

  "readFile() method" should "read data from the given location" in {
    val readFileTestDf : DataFrame = readFile(READ_LOCATION,FILE_FORMAT)(spark)
    val readFileTestDfCount : Long = readFileTestDf.count()
    assertResult(COUNT_SHOULD_BE)(readFileTestDfCount)
  }

  "readFile() method" should "throw exception in case it's not able to read data" in {
    assertThrows[FileReadException] {
      val readingFromTheWrongLocationDf : DataFrame = readFile(READ_WRONG_LOCATION, FILE_FORMAT)(spark)
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
