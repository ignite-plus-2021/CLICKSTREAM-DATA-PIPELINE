package com.igniteplus.data.pipeline.cleanseData

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{ROW_CONDITION, ROW_NUMBER}
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

object CleanData
{
  /**
   * Function to remove and filter null values and write null values to separate file
   * @param df the dataframe taken as an input
   * @param primaryColumns sequence of primary key columns
   * @param filePath the location where null values will be written
   * @param fileFormat specifies format of the file
   * @return notNullDf which is the data free from null values
   */
  def filterRemoveNull(df : DataFrame, primaryColumns : Seq[String], filePath : String, fileFormat : String) : DataFrame = {
    var nullDf : DataFrame = df
    var notNullDf : DataFrame = df
    for( i <- primaryColumns)
    {
      nullDf = df.filter(df(i).isNull)
      notNullDf = df.filter(df(i).isNotNull)
    }
    if(nullDf.count() > 0)
      writeFile(nullDf, fileFormat, filePath)
    notNullDf
  }

  def removeDuplicates (df:DataFrame ,
                        primaryKeyColumns : Seq[String],
                        orderByCol: Option[String]
                       ) : DataFrame  = {

    val dfDropDuplicates:DataFrame = orderByCol match {
      case Some(orderCol) => {
                              val windowSpec = Window.partitionBy(primaryKeyColumns.map(col):_* ).orderBy(desc(orderCol))
                              df.withColumn(colName =ROW_NUMBER, row_number().over(windowSpec))
                                .filter(conditionExpr = ROW_CONDITION ).drop(ROW_NUMBER)
                              }
      case _ => df.dropDuplicates(primaryKeyColumns)
    }

    dfDropDuplicates


  }

}
