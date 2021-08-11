package com.igniteplus.data.pipeline.cleanseData

import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, initcap, lower}

object CleanData {
  /**
   * Function to remove and filter null values and write null values to separate file
   *
   * @param df             the dataframe taken as an input
   * @param primaryColumns sequence of primary key columns
   * @param filePath       the location where null values will be written
   * @param fileFormat     specifies format of the file
   * @return notNullDf which is the data free from null values
   */
  def filterRemoveNull(df: DataFrame, primaryColumns: Seq[String], filePath: String, fileFormat: String): DataFrame = {
    var nullDf: DataFrame = df
    var notNullDf: DataFrame = df
    for (i <- primaryColumns) {
      nullDf = df.filter(df(i).isNull)
      notNullDf = df.filter(df(i).isNotNull)
    }
    if (nullDf.count() > 0)
      writeFile(nullDf, fileFormat, filePath)
    notNullDf
  }

  /**
   * CONVERT TO LOWER CASE
   * @param inputDF
   * @param columnToBeModified
   * @param columnToBeNamed
   * @return a modified Dataframe with modified case
   */
  def convertToLowerCase(inputDF: DataFrame, columnToBeModified: String, columnToBeNamed: String): DataFrame = {
    val consistentNames: DataFrame = inputDF.withColumn(columnToBeNamed, lower(col(columnToBeModified)))
    consistentNames
  }
}









