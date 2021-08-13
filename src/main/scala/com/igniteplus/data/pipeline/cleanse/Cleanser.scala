package com.igniteplus.data.pipeline.cleanse


import com.igniteplus.data.pipeline.constants.ApplicationConstants.{ROW_NUMBER, TIMESTAMP_DATATYPE, TTIMESTAMP_FORMAT}
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object Cleanser {

  /**
   * Function to to change the data type of the columns to correct datatype
   *
   * @param df the dataframe
   * @param columnNames sequence of columns of the df dataframe
   * @param dataTypes   sequence of data types
   * @return dataframe with updated data type
   */
  def dataTypeValidation(df: DataFrame, columnNames: Seq[String], dataTypes: Seq[String]): DataFrame = {
    var dfChangedDataType = df
    for (i <- columnNames.indices) {
      if (dataTypes(i) == TIMESTAMP_DATATYPE)
        dfChangedDataType = dfChangedDataType.withColumn(columnNames(i), unix_timestamp(col(columnNames(i)), TTIMESTAMP_FORMAT).cast(TIMESTAMP_DATATYPE))
      else
        dfChangedDataType = dfChangedDataType.withColumn(columnNames(i), col(columnNames(i)).cast(dataTypes(i)))
    }
    dfChangedDataType
  }


  /** @param df the dataframe taken as an input
   * @return dataframe with no whitespaces */
  def trimColumn(df: DataFrame): DataFrame = {
    var trimmedDF: DataFrame = df
    for (n <- df.columns) trimmedDF = df.withColumn(n, trim(col(n)))
    trimmedDF.show()
    trimmedDF
  }

  /**
   * Function to remove and filter null values and write null values to separate file
   *
   * @param df             the dataframe taken as an input
   * @param primaryKeyColumns sequence of primary key columns
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
   * Function to remove duplicates from the data
   *
   * @param df the dataframe
   * @param primaryKeyColumns sequence of primary key columns of the df dataframe
   * @param orderByColumn
   * @return dataframe with no duplicates
   */
  def removeDuplicates(df: DataFrame,
                       primaryKeyColumns: Seq[String],
                       orderByColumn: Option[String]
                      ): DataFrame = {

    val dfDropDuplicates: DataFrame = orderByColumn match {
      case Some(orderCol) => {
        val windowSpec = Window.partitionBy(primaryKeyColumns.map(col): _*).orderBy(desc(orderCol))
        df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
          .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
      }
      case _ => df.dropDuplicates(primaryKeyColumns)
    }
    dfDropDuplicates
  }


  /**
   * CONVERT TO LOWER CASE
   *
   * @param df the dataframe
   * @param columnTobeModified Seq of columns
   * @return a modified Dataframe with modified case
   */
  def toLowerCase(df: DataFrame, columnTobeModified: Seq[String]): DataFrame = {
    var dfLowerCase: DataFrame = df
    for (columnToModify <- columnTobeModified) {
      dfLowerCase = dfLowerCase.withColumn(df(columnToModify).toString(), lower(col(df(columnToModify).toString())))
    }
    dfLowerCase
  }

}










