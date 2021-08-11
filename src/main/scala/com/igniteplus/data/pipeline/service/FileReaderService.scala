package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.Exception.FileReadException
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReaderService {
  /**
   * Reads the contents of the file
   * @param filePath specifies the path from where the data is to read
   * @param fileType specifies the format of the file
   * @param spark
   * @return the contents read from the file
   */
    def readFile(path:String,
                 fileFormat:String)
                (implicit spark:SparkSession): DataFrame = {

        val dfReadData: DataFrame =
          try {
            spark.read
              .option("header","true")
              .option("timestampFormat", "yyyy-MM-dd HH:mm")
              .format(fileFormat)
              .load(path)
          }
          catch {
            case e: Exception =>
              FileReadException("Unable to read file from the given location " + path)
              spark.emptyDataFrame

          }

        val dfDataCount: Long = dfReadData.count()

        if(dfDataCount == 0) {

          throw FileReadException("The input file is empty " + path)

        }

        dfReadData
  }

}
