package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReaderService {
  /**
   * Reads the contents of the file
   * @param filePath specifies the path from where the data is to read
   * @param fileType specifies the format of the file
   * @param spark
   * @return the contents read from the file
   */
  def readFile(filePath : String, fileType : String)(implicit spark : SparkSession) : DataFrame = {
    val fileDf : DataFrame =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .format(fileType)
        .load(filePath)
    fileDf
  }
}
