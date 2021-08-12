package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriteException
import org.apache.spark.sql.DataFrame

object FileWriterService {
  /**
   * Function to write the data to a file
   * @param df specifies the dataframe to be written
   * @param fileType specifies the format of the file to be written
   * @param filePath specifies the path of the file
   */
  def writeFile(df: DataFrame, fileType : String, filePath : String) : Unit = {
    try {
      df.write.format(fileType)
        .option("header", "true")
        .mode("overwrite")
        .option("sep", ",")
        .save(filePath)
    }
    catch {
      case e : Exception => FileWriteException("Unable to write files to the location " + s"$filePath")
    }
  }
}

