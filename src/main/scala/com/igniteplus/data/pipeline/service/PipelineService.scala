package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.cleanse.Cleanser.{dataTypeValidation, filterRemoveNull, removeDuplicates, toLowerCase, trimColumn}
import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import com.igniteplus.data.pipeline.service.DbService.sqlWrite
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.transform.JoinTransformation
import org.apache.spark.sql.{DataFrame, SparkSession}

object PipelineService
{
  def executePipeline() (implicit spark:SparkSession) : Unit = {



    /*************** READING OF CLICK-STREAM DATA *******************************************************/
    val clickStreamDataDf: DataFrame = readFile(CLICKSTREAM_DATASET, READ_FORMAT).drop("id")

    /**************** READING OF ITEM DATA *************************************************************/
    val itemDataDf: DataFrame = readFile(ITEM_DATASET, READ_FORMAT)

    /************************** CHANGE DATATYPE *****************************************************/
    val changedDatatypeClickStreamDataDf:DataFrame = dataTypeValidation(clickStreamDataDf, COLUMNS_VALID_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM)
    val changedDatatype:DataFrame = dataTypeValidation(itemDataDf, COLUMNS_VALID_DATATYPE_ITEM, NEW_DATATYPE_ITEM)

    /************************** TRIM COLUMNS ********************************************************/
    val trimmedClickStreamDataDf:DataFrame= trimColumn(changedDatatypeClickStreamDataDf)
    val trimmedItemDf:DataFrame = trimColumn(changedDatatype)

    /***************** NULL VALUE CHECKING ************************************************************/
    val nullValueCheckClickStreamDataDf: DataFrame = filterRemoveNull(trimmedClickStreamDataDf, COLUMNS_PRIMARY_KEY_CLICKSTREAM, CLICKSTREAM_NULL_ROWS_DATASET_PATH, WRITE_FORMAT)
    val nullValueCheckItemDf: DataFrame = filterRemoveNull(trimmedItemDf, COLUMNS_PRIMARY_KEY_ITEM, ITEM_NULL_ROWS_DATASET_PATH, WRITE_FORMAT)

    /**************************** DEDUPLICAION *******************************************************************/
    val dedupliactedClickStreamDataDf:DataFrame = removeDuplicates(nullValueCheckClickStreamDataDf,COLUMNS_PRIMARY_KEY_CLICKSTREAM,Some(EVENT_TIMESTAMP_OPTION))
    val deduplicatedItemDf:DataFrame = removeDuplicates(nullValueCheckItemDf,COLUMNS_PRIMARY_KEY_ITEM,None)

    /*************************** CHANGE TO LOWER CASE ***************************************************************/
    val lowerCaseClickStreamDataDf: DataFrame = toLowerCase(dedupliactedClickStreamDataDf,COLUMNS_LOWERCASE_CLICKSTREAM)
    val lowerCaseItemDf: DataFrame = toLowerCase(deduplicatedItemDf,COLUMNS_LOWERCASE_ITEM)

    /*********************************** JOIN ***********************************************************************/
    val jointDf: DataFrame = JoinTransformation.joinTable(lowerCaseClickStreamDataDf, lowerCaseItemDf, JOIN_KEY, JOIN_TYPE_NAME)
    scala.io.StdIn.readLine()


    /*********************************** WRITING TO STAGING TABLE***********************************************************************/
    sqlWrite(jointDf,TABLE_NAME,SQL_URL_STAGING)

  }
}
