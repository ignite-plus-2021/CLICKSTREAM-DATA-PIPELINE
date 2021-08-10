package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReaderService {
  def readFile(filePath: String, fileType: String)(implicit spark: SparkSession): DataFrame = {
    val fileDf: DataFrame =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .format(fileType)
        .load(filePath)
    fileDf
  }
}
