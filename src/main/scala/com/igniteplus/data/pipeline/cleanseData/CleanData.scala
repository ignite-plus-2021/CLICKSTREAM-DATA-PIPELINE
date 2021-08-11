package com.igniteplus.data.pipeline.cleanseData


import com.igniteplus.data.pipeline.constants.ApplicationConstants.{ROW_CONDITION, ROW_NUMBER}
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, initcap, lower}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{TIMESTAMP_DATATYPE, TTIMESTAMP_FORMAT}
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, unix_timestamp}



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


  def removeDuplicates (df:DataFrame ,
                        primaryKeyColumns : Seq[String],
                        orderByColumn: Option[String]
                       ) : DataFrame  = {

    /**
     * Function to remove duplicates from the data
     * @param df the dataframe
     * @param primaryKeyColumns sequence of primary key columns of the df dataframe
     * @param orderByColumn
     * @return dataframe with no duplicates
     */

    val dfDropDuplicates: DataFrame = orderByColumn match {
      case Some(orderCol) => {
        val windowSpec = Window.partitionBy(primaryKeyColumns.map(col): _*).orderBy(desc(orderCol))
        df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
          .filter(conditionExpr = ROW_CONDITION).drop(ROW_NUMBER)
      }
      case _ => df.dropDuplicates(primaryKeyColumns)
    }

    dfDropDuplicates
  }



  def dataTypeValidation(df:DataFrame,colName:Seq[String], dt:Seq[String]): DataFrame = {
    /**
     * Function to to change the data type of the columns to correct datatype
     * @param df the dataframe
     * @param colName sequence of columns of the df dataframe
     * @param dt sequence of data types
     * @return dataframe with updated data type
     */
    var dfChangedDataType = df
    for (i <- colName.indices) {
      if (dt(i) == TIMESTAMP_DATATYPE)
        dfChangedDataType = dfChangedDataType.withColumn(colName(i), unix_timestamp(col(colName(i)), TTIMESTAMP_FORMAT).cast(TIMESTAMP_DATATYPE))
      else
        dfChangedDataType = dfChangedDataType.withColumn(colName(i), col(colName(i)).cast(dt(i)))
    }
    dfChangedDataType
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









