package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.DataFrame

object FileWriterService {
  /**
   * Writes the content specified to a file
   * @param df the dataframe that needs to be written to a file
   * @param fileType specified the format of the file
   * @param filePath specifies where the file is located
   */
  def writeFile(df:DataFrame, fileType:String, filePath:String) : Unit =
  {
    df.write.format(fileType)
      .option("header", "true")
      .mode("overwrite")
      .option("sep", ",")
      .save(filePath)
  }
}
