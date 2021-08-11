package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.DataFrame

object FileWriterService {
  /**
   * Writes the content specified to a file
   * @param df the dataframe that needs to be written to a file
   * @param fileType specified the format of the file
   * @param outputPath specifies where the file is located
   */
  def writeFile(df:DataFrame, fileType:String, outputPath:String) : Unit =
  {
    df.write.format(fileType)
      .option("header", "true")
      .mode("overwrite")
      .option("sep", ",")
      .save(outputPath)
  }
}
